package contract

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ethereum/go-ethereum/common"
)

// Updater handles updating submission counts on-chain
type Updater struct {
	client            *Client
	relayer           *RelayerClient
	submissionCounter interface{} // Will be *submission_counter.SubmissionCounter
}

// NewUpdater creates a new updater
func NewUpdater(client *Client) *Updater {
	updater := &Updater{
		client: client,
	}

	if client.GetUpdateMethod() == "relayer" {
		updater.relayer = NewRelayerClient(client.relayerURL, client.relayerAuthToken)
	}

	return updater
}

// UpdateSubmissionCounts updates submission counts for a data market
func (u *Updater) UpdateSubmissionCounts(ctx context.Context, epochID uint64, dataMarketAddress string, counts map[uint64]int, eligibleNodesCount int) error {
	// Check if contract updates are enabled
	if u.client.GetUpdateMethod() == "disabled" {
		log.Printf("Contract updates disabled (ENABLE_CONTRACT_UPDATES=false)")
		return nil
	}

	// Check if we should update for this epoch
	if !u.client.ShouldUpdate(epochID) {
		log.Printf("Skipping update for epoch %d (interval: %d)", epochID, u.client.updateEpochInterval)
		return nil
	}

	// Fetch current day
	day, err := u.client.FetchCurrentDay(ctx, common.HexToAddress(dataMarketAddress))
	if err != nil {
		log.Printf("Error: Could not fetch current day from DataMarket contract %s: %v", dataMarketAddress, err)
		log.Printf("Skipping update for epoch %d - day fetch failed", epochID)
		return fmt.Errorf("failed to fetch current day: %w", err)
	}

	// Validate day is within acceptable range (dayCounter or dayCounter - 1 per contract requirement)
	// Note: We can't validate against dayCounter here since we'd need another call, but we log it for debugging
	log.Printf("Fetched current day %s for data market %s (epoch %d)", day.String(), dataMarketAddress, epochID)

	// Convert counts to sorted arrays
	slotIDs := make([]*big.Int, 0)
	submissionsList := make([]*big.Int, 0)

	// Sort by slotID for consistency
	slotKeys := make([]uint64, 0, len(counts))
	for slotID := range counts {
		slotKeys = append(slotKeys, slotID)
	}
	sort.Slice(slotKeys, func(i, j int) bool {
		return slotKeys[i] < slotKeys[j]
	})

	for _, slotID := range slotKeys {
		count := counts[slotID]
		if count > 0 {
			slotIDs = append(slotIDs, big.NewInt(int64(slotID)))
			submissionsList = append(submissionsList, big.NewInt(int64(count)))
		}
	}

	if len(slotIDs) == 0 {
		log.Printf("No submissions to update for data market %s on day %s", dataMarketAddress, day.String())
		return nil
	}

	batchSize := u.client.batchSize
	totalSlots := len(slotIDs)
	log.Printf("Updating submission counts for epoch %d, data market %s, day %s: %d slots (batch size: %d)",
		epochID, dataMarketAddress, day.String(), totalSlots, batchSize)

	// Batch and send updates
	if u.client.GetUpdateMethod() == "relayer" {
		// Calculate number of batches
		numBatches := (totalSlots + batchSize - 1) / batchSize
		if numBatches > 1 {
			log.Printf("Splitting %d slots into %d batches", totalSlots, numBatches)
		}

		var wg sync.WaitGroup
		errChan := make(chan error, numBatches)
		var firstError error
		var mu sync.Mutex

		// Process batches
		for start := 0; start < totalSlots; start += batchSize {
			end := start + batchSize
			if end > totalSlots {
				end = totalSlots
			}

			batchSlotIDs := slotIDs[start:end]
			batchSubmissions := submissionsList[start:end]
			batchNum := (start / batchSize) + 1

			wg.Add(1)
			go func(batchNum, start, end int, batchSlotIDs, batchSubmissions []*big.Int) {
				defer wg.Done()

				// Create context with timeout for this batch
				batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				// Use retry logic for each batch
				operation := func() error {
					// For periodic updates (eligibleNodesCount == 0), use updateSubmissionCounts
					if eligibleNodesCount == 0 {
						return u.relayer.SendUpdateSubmissionCounts(batchCtx, dataMarketAddress, batchSlotIDs, batchSubmissions, day)
					} else {
						// For backward compatibility, use updateRewards if eligibleNodesCount > 0
						return u.relayer.SendUpdateRewards(batchCtx, dataMarketAddress, batchSlotIDs, batchSubmissions, day, eligibleNodesCount)
					}
				}

				backoffConfig := backoff.NewExponentialBackOff()
				backoffConfig.InitialInterval = 1 * time.Second
				backoffConfig.Multiplier = 1.5
				backoffConfig.MaxInterval = 4 * time.Second
				backoffConfig.MaxElapsedTime = 10 * time.Second

				if err := backoff.Retry(operation, backoff.WithContext(backoffConfig, batchCtx)); err != nil {
					mu.Lock()
					if firstError == nil {
						firstError = fmt.Errorf("batch %d (%d-%d): %w", batchNum, start, end, err)
					}
					mu.Unlock()
					errChan <- err
					log.Printf("❌ Failed to send batch %d (%d-%d) for epoch %d: %v", batchNum, start, end, epochID, err)
					return
				}

				log.Printf("✅ Successfully sent batch %d/%d (%d-%d slots) for epoch %d", batchNum, numBatches, start, end, epochID)
			}(batchNum, start, end, batchSlotIDs, batchSubmissions)

			// Small delay between batch starts to prevent overwhelming the relayer
			if start+batchSize < totalSlots {
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Wait for all batches to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		errorCount := 0
		for range errChan {
			errorCount++
		}

		if errorCount > 0 {
			if firstError != nil {
				return fmt.Errorf("failed to send %d/%d batches: %w", errorCount, numBatches, firstError)
			}
			return fmt.Errorf("failed to send %d/%d batches", errorCount, numBatches)
		}

		log.Printf("✅ Successfully sent all %d batches for epoch %d, data market %s", numBatches, epochID, dataMarketAddress)
		return nil
	} else {
		return fmt.Errorf("direct contract calls not implemented - use relayer method")
	}
}

// UpdateFinalRewards sends final reward update for a previous day at buffer epoch
func (u *Updater) UpdateFinalRewards(ctx context.Context, currentEpoch uint64, dataMarketAddress string, day string, counts map[uint64]int, eligibleNodesCount int) error {
	log.Printf("🎯 UpdateFinalRewards: ENTRY - epoch=%d, dataMarket=%s, day=%s, eligibleNodes=%d, slotCount=%d",
		currentEpoch, dataMarketAddress, day, eligibleNodesCount, len(counts))

	// Check if contract updates are enabled
	if u.client.GetUpdateMethod() == "disabled" {
		log.Printf("❌ UpdateFinalRewards: Contract updates disabled (ENABLE_CONTRACT_UPDATES=false) - exiting early")
		return nil
	}

	// Parse day string to big.Int
	dayBigInt, ok := new(big.Int).SetString(day, 10)
	if !ok {
		return fmt.Errorf("invalid day string: %s", day)
	}

	// Convert counts to sorted arrays
	slotIDs := make([]*big.Int, 0)
	submissionsList := make([]*big.Int, 0)

	// Sort by slotID for consistency
	slotKeys := make([]uint64, 0, len(counts))
	for slotID := range counts {
		slotKeys = append(slotKeys, slotID)
	}
	sort.Slice(slotKeys, func(i, j int) bool {
		return slotKeys[i] < slotKeys[j]
	})

	for _, slotID := range slotKeys {
		count := counts[slotID]
		if count > 0 {
			slotIDs = append(slotIDs, big.NewInt(int64(slotID)))
			submissionsList = append(submissionsList, big.NewInt(int64(count)))
		}
	}

	if len(slotIDs) == 0 {
		log.Printf("⚠️ UpdateFinalRewards: No submissions to update for final rewards: data market %s, day %s - exiting early", dataMarketAddress, day)
		return nil
	}

	batchSize := u.client.batchSize
	totalSlots := len(slotIDs)
	log.Printf("Sending final rewards update for epoch %d, data market %s, day %s: %d slots, eligibleNodes=%d (batch size: %d)",
		currentEpoch, dataMarketAddress, day, totalSlots, eligibleNodesCount, batchSize)

	if u.client.GetUpdateMethod() == "relayer" {
		// Step 1: Update eligible nodes for the day (single call, no batching needed)
		operation1 := func() error {
			return u.relayer.SendUpdateEligibleNodes(ctx, dataMarketAddress, dayBigInt, eligibleNodesCount)
		}

		backoffConfig1 := backoff.NewExponentialBackOff()
		backoffConfig1.InitialInterval = 1 * time.Second
		backoffConfig1.Multiplier = 1.5
		backoffConfig1.MaxInterval = 4 * time.Second
		backoffConfig1.MaxElapsedTime = 10 * time.Second

		if err := backoff.Retry(operation1, backoff.WithContext(backoffConfig1, ctx)); err != nil {
			return fmt.Errorf("failed to update eligible nodes (step 1) after retries: %w", err)
		}

		log.Printf("✅ Step 1 complete: Updated eligible nodes count (%d) for day %s", eligibleNodesCount, day)

		// Wait before starting Step 2 to allow Step 1 transaction to be mined
		// This prevents gas estimation failures when Step 2 calls fire too rapidly
		stepDelay := u.client.step1ToStep2Delay
		if stepDelay > 0 {
			log.Printf("⏳ Waiting %v before starting Step 2 (allowing Step 1 transaction to be mined)...", stepDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(stepDelay):
				log.Printf("✅ Delay complete, starting Step 2 batches")
			}
		}

		// Step 2: Update eligible submission counts and distribute rewards (batched)
		if totalSlots == 0 {
			log.Printf("No slots to update for final rewards: data market %s, day %s", dataMarketAddress, day)
			return nil
		}

		numBatches := (totalSlots + batchSize - 1) / batchSize
		if numBatches > 1 {
			log.Printf("Splitting %d slots into %d batches for step 2", totalSlots, numBatches)
		}

		var wg sync.WaitGroup
		errChan := make(chan error, numBatches)
		var firstError error
		var mu sync.Mutex

		// Process batches for step 2
		for start := 0; start < totalSlots; start += batchSize {
			end := start + batchSize
			if end > totalSlots {
				end = totalSlots
			}

			batchSlotIDs := slotIDs[start:end]
			batchSubmissions := submissionsList[start:end]
			batchNum := (start / batchSize) + 1

			log.Printf("📦 [Step 2] Preparing batch %d/%d: day=%s, slots=%d-%d (total=%d slots)",
				batchNum, numBatches, day, start, end-1, len(batchSlotIDs))

			wg.Add(1)
			go func(batchNum, start, end int, batchSlotIDs, batchSubmissions []*big.Int) {
				defer wg.Done()

				// Create context with timeout for this batch
				batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				// Use retry logic for each batch
				operation2 := func() error {
					log.Printf("🚀 [Step 2] Sending batch %d/%d to relayer: day=%s, slots=%d", batchNum, numBatches, day, len(batchSlotIDs))
					return u.relayer.SendUpdateEligibleSubmissionCounts(batchCtx, dataMarketAddress, batchSlotIDs, batchSubmissions, dayBigInt)
				}

				backoffConfig2 := backoff.NewExponentialBackOff()
				backoffConfig2.InitialInterval = 1 * time.Second
				backoffConfig2.Multiplier = 1.5
				backoffConfig2.MaxInterval = 4 * time.Second
				backoffConfig2.MaxElapsedTime = 10 * time.Second

				if err := backoff.Retry(operation2, backoff.WithContext(backoffConfig2, batchCtx)); err != nil {
					mu.Lock()
					if firstError == nil {
						firstError = fmt.Errorf("batch %d (%d-%d): %w", batchNum, start, end, err)
					}
					mu.Unlock()
					errChan <- err
					log.Printf("❌ Failed to send step 2 batch %d (%d-%d) for epoch %d: %v", batchNum, start, end, currentEpoch, err)
					return
				}

				log.Printf("✅ Successfully sent step 2 batch %d/%d (%d-%d slots) for epoch %d", batchNum, numBatches, start, end, currentEpoch)
			}(batchNum, start, end, batchSlotIDs, batchSubmissions)

			// Small delay between batch starts to prevent overwhelming the relayer
			if start+batchSize < totalSlots {
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Wait for all batches to complete
		wg.Wait()
		close(errChan)

		// Check for errors
		errorCount := 0
		for range errChan {
			errorCount++
		}

		if errorCount > 0 {
			if firstError != nil {
				return fmt.Errorf("failed to send %d/%d batches in step 2: %w", errorCount, numBatches, firstError)
			}
			return fmt.Errorf("failed to send %d/%d batches in step 2", errorCount, numBatches)
		}

		log.Printf("✅ Successfully sent all %d batches for step 2 (final rewards update) for epoch %d, data market %s, day %s",
			numBatches, currentEpoch, dataMarketAddress, day)
		return nil
	} else {
		return fmt.Errorf("direct contract calls not implemented - use relayer method")
	}
}
