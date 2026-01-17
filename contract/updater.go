package contract

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sort"
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

	log.Printf("Updating submission counts for epoch %d, data market %s, day %s: %d slots",
		epochID, dataMarketAddress, day.String(), len(slotIDs))

	// Use retry logic
	operation := func() error {
		if u.client.GetUpdateMethod() == "relayer" {
			// For periodic updates (eligibleNodesCount == 0), use updateSubmissionCounts
			if eligibleNodesCount == 0 {
				return u.relayer.SendUpdateSubmissionCounts(ctx, dataMarketAddress, slotIDs, submissionsList, day)
			} else {
				// For backward compatibility, use updateRewards if eligibleNodesCount > 0
				// In the future, this should be replaced with the two-step process
				return u.relayer.SendUpdateRewards(ctx, dataMarketAddress, slotIDs, submissionsList, day, eligibleNodesCount)
			}
		} else {
			return fmt.Errorf("direct contract calls not implemented - use relayer method")
		}
	}

	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 1 * time.Second
	backoffConfig.Multiplier = 1.5
	backoffConfig.MaxInterval = 4 * time.Second
	backoffConfig.MaxElapsedTime = 10 * time.Second

	if err := backoff.Retry(operation, backoff.WithContext(backoffConfig, ctx)); err != nil {
		return fmt.Errorf("failed to update submission counts after retries: %w", err)
	}

	log.Printf("Successfully updated submission counts for epoch %d, data market %s", epochID, dataMarketAddress)
	return nil
}

// UpdateFinalRewards sends final reward update for a previous day at buffer epoch
func (u *Updater) UpdateFinalRewards(ctx context.Context, currentEpoch uint64, dataMarketAddress string, day string, counts map[uint64]int, eligibleNodesCount int) error {
	// Check if contract updates are enabled
	if u.client.GetUpdateMethod() == "disabled" {
		log.Printf("Contract updates disabled (ENABLE_CONTRACT_UPDATES=false)")
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
		log.Printf("No submissions to update for final rewards: data market %s, day %s", dataMarketAddress, day)
		return nil
	}

	log.Printf("Sending final rewards update for epoch %d, data market %s, day %s: %d slots, eligibleNodes=%d",
		currentEpoch, dataMarketAddress, day, len(slotIDs), eligibleNodesCount)

	// Use retry logic - two-step process for end-of-day updates
	operation := func() error {
		if u.client.GetUpdateMethod() == "relayer" {
			// Step 1: Update eligible nodes for the day
			if err := u.relayer.SendUpdateEligibleNodes(ctx, dataMarketAddress, dayBigInt, eligibleNodesCount); err != nil {
				return fmt.Errorf("failed to update eligible nodes (step 1): %w", err)
			}

			// Step 2: Update eligible submission counts and distribute rewards
			if err := u.relayer.SendUpdateEligibleSubmissionCounts(ctx, dataMarketAddress, slotIDs, submissionsList, dayBigInt); err != nil {
				return fmt.Errorf("failed to update eligible submission counts (step 2): %w", err)
			}

			return nil
		} else {
			return fmt.Errorf("direct contract calls not implemented - use relayer method")
		}
	}

	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 1 * time.Second
	backoffConfig.Multiplier = 1.5
	backoffConfig.MaxInterval = 4 * time.Second
	backoffConfig.MaxElapsedTime = 10 * time.Second

	if err := backoff.Retry(operation, backoff.WithContext(backoffConfig, ctx)); err != nil {
		return fmt.Errorf("failed to send final rewards update after retries: %w", err)
	}

	log.Printf("Successfully sent final rewards update for epoch %d, data market %s, day %s", currentEpoch, dataMarketAddress, day)
	return nil
}
