package main

import (
	"log"
	"sync"
)

// SubmissionCounter tracks eligible submission counts per slot and data market
type SubmissionCounter struct {
	counts map[string]map[uint64]int // dataMarketAddress -> slotID -> count
	mu     sync.RWMutex
}

// NewSubmissionCounter creates a new submission counter
func NewSubmissionCounter() *SubmissionCounter {
	return &SubmissionCounter{
		counts: make(map[string]map[uint64]int),
	}
}

// ExtractSubmissionCounts extracts slot ID counts from a finalized batch for a specific data market
// Returns map[slotID]count for the given dataMarket
// Count represents how many unique projects each slot submitted to with >51% validator votes
// Only submissions with majority votes (>51% of validators) are counted
func ExtractSubmissionCounts(batch *FinalizedBatch, dataMarket string) (map[uint64]int, error) {
	totalValidators := batch.ValidatorCount
	if totalValidators == 0 {
		// Fallback: if ValidatorCount is 0, try to infer from batches
		// This shouldn't happen after consensus, but handle gracefully
		totalValidators = 1
	}

	// Calculate majority threshold (>51%)
	majorityThreshold := float64(totalValidators) * 0.51
	majorityVotes := int(majorityThreshold) + 1 // Need more than 51%

	// Track slot ID -> set of projects they submitted to with majority votes
	slotProjects := make(map[uint64]map[string]bool) // slotID -> set of projectIDs

	// Iterate through SubmissionDetails and track which projects each slot submitted to
	// Only count submissions where VoteCount > majorityThreshold
	for projectID, submissions := range batch.SubmissionDetails {
		for _, submission := range submissions {
			// Only count if this submission has majority votes (>51%)
			if submission.VoteCount > majorityVotes {
				slotID := submission.SlotID
				if slotProjects[slotID] == nil {
					slotProjects[slotID] = make(map[string]bool)
				}
				slotProjects[slotID][projectID] = true
			}
		}
	}

	// Convert to counts map: count = number of unique projects this slot submitted to with majority votes
	counts := make(map[uint64]int)
	for slotID, projects := range slotProjects {
		counts[slotID] = len(projects)
	}

	log.Printf("Extracted submission counts for dataMarket %s: %d slots with majority votes (threshold: >%d/%d validators)",
		dataMarket, len(counts), majorityVotes, totalValidators)

	return counts, nil
}

// UpdateEligibleCounts updates the internal tracking of eligible counts for a specific data market
func (sc *SubmissionCounter) UpdateEligibleCounts(epochID uint64, dataMarket string, slotCounts map[uint64]int) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.counts[dataMarket] == nil {
		sc.counts[dataMarket] = make(map[uint64]int)
	}

	for slotID, count := range slotCounts {
		// Accumulate counts (or replace, depending on requirements)
		sc.counts[dataMarket][slotID] += count
	}

	log.Printf("Updated eligible counts for epoch %d, dataMarket %s: %d slots",
		epochID, dataMarket, len(slotCounts))

	return nil
}

// GetCounts returns the current counts for a data market
func (sc *SubmissionCounter) GetCounts(dataMarket string) map[uint64]int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if counts, ok := sc.counts[dataMarket]; ok {
		// Return a copy
		result := make(map[uint64]int)
		for slotID, count := range counts {
			result[slotID] = count
		}
		return result
	}

	return make(map[uint64]int)
}

// GetAllCounts returns all counts across all data markets
func (sc *SubmissionCounter) GetAllCounts() map[string]map[uint64]int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	result := make(map[string]map[uint64]int)
	for dataMarket, slotCounts := range sc.counts {
		result[dataMarket] = make(map[uint64]int)
		for slotID, count := range slotCounts {
			result[dataMarket][slotID] = count
		}
	}

	return result
}

// GetEligibleNodesCount returns the number of slots with eligible submissions (>0 projects with majority votes)
// A slot is eligible if it has at least one submission with >51% validator votes
func (sc *SubmissionCounter) GetEligibleNodesCount(dataMarket string) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	count := 0
	if slotCounts, ok := sc.counts[dataMarket]; ok {
		for _, submissionCount := range slotCounts {
			// Count slots that have at least one project submission with majority votes
			if submissionCount > 0 {
				count++
			}
		}
	}

	return count
}

// ResetCounts resets counts for a data market (useful for day transitions)
func (sc *SubmissionCounter) ResetCounts(dataMarket string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.counts, dataMarket)
	log.Printf("Reset counts for data market %s", dataMarket)
}

// Helper function
func getTotalSlots(counts map[string]map[uint64]int) int {
	total := 0
	for _, slotCounts := range counts {
		total += len(slotCounts)
	}
	return total
}

