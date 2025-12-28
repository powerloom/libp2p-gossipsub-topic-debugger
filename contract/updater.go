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
		// If FetchCurrentDay is not implemented, we'll need to handle this differently
		// For now, use a placeholder day (in production, this should be fetched from contract or cache)
		log.Printf("Warning: Could not fetch current day, using epoch-based day calculation")
		day = big.NewInt(int64(epochID / 288)) // Rough estimate: 288 epochs per day
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
		log.Printf("No submissions to update for data market %s on day %s", dataMarketAddress, day.String())
		return nil
	}

	log.Printf("Updating submission counts for epoch %d, data market %s, day %s: %d slots",
		epochID, dataMarketAddress, day.String(), len(slotIDs))

	// Use retry logic
	operation := func() error {
		if u.client.GetUpdateMethod() == "relayer" {
			return u.relayer.SendUpdateRewards(ctx, dataMarketAddress, slotIDs, submissionsList, day, eligibleNodesCount)
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
