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
func ExtractSubmissionCounts(batch *FinalizedBatch, dataMarket string) (map[uint64]int, error) {
	counts := make(map[uint64]int)

	// Iterate through SubmissionDetails and count submissions per slot
	for _, submissions := range batch.SubmissionDetails {
		for _, submission := range submissions {
			slotID := submission.SlotID
			counts[slotID]++
		}
	}

	log.Printf("Extracted submission counts for dataMarket %s: %d slots",
		dataMarket, len(counts))

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

// GetEligibleNodesCount returns the number of slots with submissions > 0 for a data market
func (sc *SubmissionCounter) GetEligibleNodesCount(dataMarket string) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	count := 0
	if slotCounts, ok := sc.counts[dataMarket]; ok {
		for _, submissionCount := range slotCounts {
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
