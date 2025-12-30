package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
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
// Count represents how many unique projects each slot submitted to
// Note: This is used for aggregated batches (after consensus filtering to winning CIDs)
func ExtractSubmissionCounts(batch *FinalizedBatch, dataMarket string) (map[uint64]int, error) {
	totalValidators := batch.ValidatorCount
	if totalValidators == 0 {
		// Fallback: if ValidatorCount is 0, try to infer from batches
		// This shouldn't happen after consensus, but handle gracefully
		totalValidators = 1
	}

	// Track slot ID -> set of projects they submitted to
	// We count all submissions that appear in the aggregated batch (winning CID),
	// regardless of how many validators saw them, since they're already part of consensus
	slotProjects := make(map[uint64]map[string]bool) // slotID -> set of projectIDs

	// Iterate through SubmissionDetails and track which projects each slot submitted to
	// All submissions in the aggregated batch are counted (they're part of winning CIDs)
	for projectID, submissions := range batch.SubmissionDetails {
		for _, submission := range submissions {
			// Count all submissions that appear in aggregated batch
			// They're already part of the winning CID, so they're valid submissions
			slotID := submission.SlotID
			if slotProjects[slotID] == nil {
				slotProjects[slotID] = make(map[string]bool)
			}
			slotProjects[slotID][projectID] = true
		}
	}

	// Convert to counts map: count = number of unique projects this slot submitted to
	counts := make(map[uint64]int)
	for slotID, projects := range slotProjects {
		counts[slotID] = len(projects)
	}

	log.Printf("Extracted submission counts for dataMarket %s: %d slots (total validators: %d)",
		dataMarket, len(counts), totalValidators)

	return counts, nil
}

// ExtractSubmissionCountsFromBatches extracts slot ID counts from ALL Level 1 batches
// Eligibility criteria: A slot is eligible for a project if:
//  1. The winning CID for that project (from aggregated batch consensus) matches the slot's submission CID
//  2. The slot's submission (slotID + snapshotCID) appears in submission_details[projectID] of at least one Level 1 batch
//  3. The slot has >51% votes from validators who reported that (projectID, CID) combination
//
// Denominator: Number of validators that reported that (projectID, CID) combination
// This ensures slots are only counted for projects where their CID won consensus
func ExtractSubmissionCountsFromBatches(batches []*FinalizedBatch, aggregatedBatch *FinalizedBatch, dataMarket string) (map[uint64]int, error) {
	if len(batches) == 0 || aggregatedBatch == nil {
		return make(map[uint64]int), nil
	}

	// Build map of winning (projectID -> CID) from aggregated batch
	winningProjectCIDs := make(map[string]string)
	for i, projectID := range aggregatedBatch.ProjectIds {
		if i < len(aggregatedBatch.SnapshotCids) {
			winningProjectCIDs[projectID] = aggregatedBatch.SnapshotCids[i]
		}
	}

	// Track (projectID, CID) -> set of validators who chose that CID (denominator)
	// Only track for winning CIDs
	projectCIDValidators := make(map[string]map[string]bool) // key: "projectID:cid" -> set of validator IDs

	// Track (slotID, projectID, CID) -> set of validators who saw that submission (numerator)
	// Only track for winning CIDs
	slotProjectCIDValidators := make(map[string]map[string]bool) // key: "slotID:projectID:cid" -> set of validator IDs

	// First pass: identify validators who chose the winning CID for each project (denominator)
	for _, batch := range batches {
		validatorID := batch.SequencerId
		if validatorID == "" {
			continue
		}

		// Map projectID to CID from this batch's SnapshotCids array
		projectToCID := make(map[string]string)
		for i, projectID := range batch.ProjectIds {
			if i < len(batch.SnapshotCids) {
				projectToCID[projectID] = batch.SnapshotCids[i]
			}
		}

		// Track which validators chose the winning CID for each project
		for projectID, cid := range projectToCID {
			if cid == "" {
				continue
			}
			winningCID, isWinningProject := winningProjectCIDs[projectID]
			if !isWinningProject || cid != winningCID {
				continue
			}

			key := fmt.Sprintf("%s:%s", projectID, winningCID)
			if projectCIDValidators[key] == nil {
				projectCIDValidators[key] = make(map[string]bool)
			}
			projectCIDValidators[key][validatorID] = true
		}
	}

	// Second pass: check ALL batches for submissions matching the winning CID
	// This ensures we find submissions even if the validator chose a different CID
	for _, batch := range batches {
		validatorID := batch.SequencerId
		if validatorID == "" {
			continue
		}

		// Check all submission_details for submissions matching winning CIDs
		for projectID, submissions := range batch.SubmissionDetails {
			winningCID, isWinningProject := winningProjectCIDs[projectID]
			if !isWinningProject {
				continue
			}

			// Check all submissions for this project
			for _, submission := range submissions {
				// If this submission matches the winning CID, track it
				if submission.SnapshotCID == winningCID {
					slotKey := fmt.Sprintf("%d:%s:%s", submission.SlotID, projectID, winningCID)
					if slotProjectCIDValidators[slotKey] == nil {
						slotProjectCIDValidators[slotKey] = make(map[string]bool)
					}
					slotProjectCIDValidators[slotKey][validatorID] = true
				}
			}
		}
	}

	// Second pass: count eligible projects per slot
	// A slot is eligible for a project if it has >51% votes from validators who reported that project+CID
	slotProjects := make(map[uint64]map[string]bool) // slotID -> set of projectIDs

	for slotKey, slotValidators := range slotProjectCIDValidators {
		// Parse slotKey: "slotID:projectID:cid"
		// Note: projectID may contain colons, so we need to parse carefully
		// Format is: slotID (numeric) : projectID (may contain colons) : cid (last part)
		parts := strings.Split(slotKey, ":")
		if len(parts) < 3 {
			continue
		}
		slotIDStr := parts[0]
		cid := parts[len(parts)-1]
		projectID := strings.Join(parts[1:len(parts)-1], ":") // Rejoin middle parts as projectID

		slotID, err := strconv.ParseUint(slotIDStr, 10, 64)
		if err != nil {
			continue
		}

		// Get denominator: validators who reported this (projectID, CID)
		projectCIDKey := fmt.Sprintf("%s:%s", projectID, cid)
		denominatorValidators, exists := projectCIDValidators[projectCIDKey]
		if !exists || len(denominatorValidators) == 0 {
			continue
		}

		// Calculate majority threshold (>51% of validators who reported this project+CID)
		denominator := len(denominatorValidators)
		majorityThreshold := float64(denominator) * 0.51
		majorityVotes := int(majorityThreshold) + 1 // Need more than 51%

		// Check if slot has majority votes
		numerator := len(slotValidators)
		if numerator > majorityVotes {
			if slotProjects[slotID] == nil {
				slotProjects[slotID] = make(map[string]bool)
			}
			slotProjects[slotID][projectID] = true
		}
	}

	// Convert to counts map: count = number of unique projects this slot is eligible for
	counts := make(map[uint64]int)
	for slotID, projects := range slotProjects {
		counts[slotID] = len(projects)
	}

	log.Printf("Extracted submission counts from %d Level 1 batches for dataMarket %s: %d eligible slots (winning projects: %d)",
		len(batches), dataMarket, len(counts), len(winningProjectCIDs))

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
