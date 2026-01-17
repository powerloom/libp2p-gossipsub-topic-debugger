package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"

	"p2p-debugger/redis"
)

// SubmissionCounter tracks eligible submission counts per slot, data market, and day
// Counts are persisted to Redis for epoch-over-epoch persistence
type SubmissionCounter struct {
	counts map[string]map[string]map[uint64]int // dataMarketAddress -> day -> slotID -> count
	mu     sync.RWMutex
	ctx    context.Context
}

// NewSubmissionCounter creates a new submission counter
func NewSubmissionCounter(ctx context.Context) *SubmissionCounter {
	return &SubmissionCounter{
		counts: make(map[string]map[string]map[uint64]int),
		ctx:    ctx,
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

	// Track (projectID, CID) -> set of validators who REPORTED that (projectID, CID) combination (denominator)
	// This includes validators who saw submissions with that CID, regardless of which CID they chose
	// Only track for winning CIDs
	projectCIDValidators := make(map[string]map[string]bool) // key: "projectID:cid" -> set of validator IDs

	// Track (slotID, projectID, CID) -> set of validators who saw that submission (numerator)
	// Only track for winning CIDs
	slotProjectCIDValidators := make(map[string]map[string]bool) // key: "slotID:projectID:cid" -> set of validator IDs

	// Single pass: check ALL batches for submissions matching winning CIDs
	// Track both: (1) which validators reported each (projectID, winningCID) combination, and
	//             (2) which validators saw each (slotID, projectID, winningCID) combination
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
					// Track denominator: validators who reported this (projectID, winningCID) combination
					projectCIDKey := fmt.Sprintf("%s:%s", projectID, winningCID)
					if projectCIDValidators[projectCIDKey] == nil {
						projectCIDValidators[projectCIDKey] = make(map[string]bool)
					}
					projectCIDValidators[projectCIDKey][validatorID] = true

					// Track numerator: validators who saw this (slotID, projectID, winningCID) combination
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
		// For >51%, we need at least ceil(threshold) votes
		// Examples: denominator=1 -> ceil(0.51)=1, denominator=2 -> ceil(1.02)=2, denominator=3 -> ceil(1.53)=2
		majorityVotes := int(math.Ceil(majorityThreshold))

		// Check if slot has majority votes
		numerator := len(slotValidators)
		// Use >= to handle edge cases (e.g., denominator=1 means 100% which is definitely >51%)
		if numerator >= majorityVotes && numerator > 0 {
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

// UpdateEligibleCounts updates the internal tracking of eligible counts for a specific data market and day
func (sc *SubmissionCounter) UpdateEligibleCounts(epochID uint64, dataMarket string, slotCounts map[uint64]int) error {
	return sc.UpdateEligibleCountsForDay(epochID, dataMarket, "", slotCounts)
}

// UpdateEligibleCountsForDay updates the internal tracking of eligible counts for a specific data market and day
// Persists counts to Redis for epoch-over-epoch persistence
func (sc *SubmissionCounter) UpdateEligibleCountsForDay(epochID uint64, dataMarket string, day string, slotCounts map[uint64]int) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.counts[dataMarket] == nil {
		sc.counts[dataMarket] = make(map[string]map[uint64]int)
	}

	if sc.counts[dataMarket][day] == nil {
		sc.counts[dataMarket][day] = make(map[uint64]int)
	}

	// Update in-memory counts and persist to Redis
	for slotID, count := range slotCounts {
		// Accumulate counts per day in memory
		sc.counts[dataMarket][day][slotID] += count

		// Persist to Redis: increment slot submission count by the actual count value
		// count represents the number of unique projects this slot submitted to in this epoch
		slotIDStr := strconv.FormatUint(slotID, 10)
		key := redis.SlotSubmissionKey(dataMarket, slotIDStr, day)

		// Increment Redis counter by the actual count value (not just 1)
		newCount, err := redis.IncrBy(sc.ctx, key, int64(count))
		if err != nil {
			log.Printf("⚠️ Failed to persist submission count to Redis for slot %d, dataMarket %s, day %s: %v", slotID, dataMarket, day, err)
			// Continue with in-memory update even if Redis fails
			// Use in-memory count as fallback
			newCount = int64(sc.counts[dataMarket][day][slotID])
		} else {
			log.Printf("📈 Redis: Slot %d submission count incremented by %d for dataMarket %s, day %s: new total = %d", slotID, count, dataMarket, day, newCount)
		}

		// Also update EligibleSlotSubmissionKey with same value (submissions are already eligible)
		eligibleKey := redis.EligibleSlotSubmissionKey(dataMarket, slotIDStr, day)
		if err := redis.Set(sc.ctx, eligibleKey, strconv.FormatInt(newCount, 10)); err != nil {
			log.Printf("⚠️ Failed to update EligibleSlotSubmissionKey for slot %d, dataMarket %s, day %s: %v", slotID, dataMarket, day, err)
		}

		// Also track in eligible slot submissions hash by epoch (for final rewards calculation)
		epochIDStr := strconv.FormatUint(epochID, 10)
		epochKey := redis.EligibleSlotSubmissionsByEpochKey(dataMarket, day, epochIDStr)
		if err := redis.HSet(sc.ctx, epochKey, slotIDStr, strconv.FormatInt(newCount, 10)); err != nil {
			log.Printf("⚠️ Failed to persist epoch submission count to Redis for slot %d, epoch %d: %v", slotID, epochID, err)
		}

		// Track eligible nodes set (slots with >0 submissions)
		// Note: Final eligibility (>= quota) is checked at final rewards time
		// We add all slots with submissions here, but GetEligibleNodesCountForDay filters by quota
		if newCount > 0 {
			eligibleNodesKey := redis.EligibleNodesByDayKey(dataMarket, day)
			if err := redis.SAdd(sc.ctx, eligibleNodesKey, slotIDStr); err != nil {
				log.Printf("⚠️ Failed to add slot %d to eligible nodes set: %v", slotID, err)
			}
		}
	}

	log.Printf("Updated eligible counts for epoch %d, dataMarket %s, day %s: %d slots",
		epochID, dataMarket, day, len(slotCounts))

	return nil
}

// GetCounts returns the current counts for a data market (all days combined)
func (sc *SubmissionCounter) GetCounts(dataMarket string) map[uint64]int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	result := make(map[uint64]int)
	if dayCounts, ok := sc.counts[dataMarket]; ok {
		// Combine counts from all days
		for _, slotCounts := range dayCounts {
			for slotID, count := range slotCounts {
				result[slotID] += count
			}
		}
	}

	return result
}

// GetCountsForDay returns the counts for a specific day
// Reads from Redis first (source of truth), falls back to in-memory cache
func (sc *SubmissionCounter) GetCountsForDay(dataMarket string, day string) map[uint64]int {
	// Try Redis first (source of truth)
	if redis.RedisClient != nil {
		// Get all eligible slots for this day
		eligibleNodesKey := redis.EligibleNodesByDayKey(dataMarket, day)
		slotIDs, err := redis.SMembers(sc.ctx, eligibleNodesKey)
		if err == nil && len(slotIDs) > 0 {
			result := make(map[uint64]int)
			for _, slotIDStr := range slotIDs {
				slotID, err := strconv.ParseUint(slotIDStr, 10, 64)
				if err != nil {
					continue
				}
				// Read from EligibleSlotSubmissionKey (or SlotSubmissionKey as fallback)
				eligibleKey := redis.EligibleSlotSubmissionKey(dataMarket, slotIDStr, day)
				countStr, err := redis.Get(sc.ctx, eligibleKey)
				if err == nil && countStr != "" {
					if count, err := strconv.Atoi(countStr); err == nil {
						result[slotID] = count
					}
				} else {
					// Fallback to SlotSubmissionKey
					slotKey := redis.SlotSubmissionKey(dataMarket, slotIDStr, day)
					countStr, err := redis.Get(sc.ctx, slotKey)
					if err == nil && countStr != "" {
						if count, err := strconv.Atoi(countStr); err == nil {
							result[slotID] = count
						}
					}
				}
			}
			if len(result) > 0 {
				return result
			}
		}
	}

	// Fallback to in-memory cache
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if dayCounts, ok := sc.counts[dataMarket]; ok {
		if slotCounts, ok := dayCounts[day]; ok {
			// Return a copy
			result := make(map[uint64]int)
			for slotID, count := range slotCounts {
				result[slotID] = count
			}
			return result
		}
	}

	return make(map[uint64]int)
}

// GetAllCounts returns all counts across all data markets (all days combined)
func (sc *SubmissionCounter) GetAllCounts() map[string]map[uint64]int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	result := make(map[string]map[uint64]int)
	for dataMarket, dayCounts := range sc.counts {
		result[dataMarket] = make(map[uint64]int)
		// Combine counts from all days
		for _, slotCounts := range dayCounts {
			for slotID, count := range slotCounts {
				result[dataMarket][slotID] += count
			}
		}
	}

	return result
}

// GetEligibleNodesCount returns the number of slots with eligible submissions (>0 projects with majority votes)
// A slot is eligible if it has at least one submission with >51% validator votes
// Returns count for all days combined
func (sc *SubmissionCounter) GetEligibleNodesCount(dataMarket string) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	eligibleSlots := make(map[uint64]bool)
	if dayCounts, ok := sc.counts[dataMarket]; ok {
		for _, slotCounts := range dayCounts {
			for slotID, submissionCount := range slotCounts {
				if submissionCount > 0 {
					eligibleSlots[slotID] = true
				}
			}
		}
	}

	return len(eligibleSlots)
}

// GetEligibleNodesCountForDay returns the number of slots with eligible submissions >= dailySnapshotQuota for a specific day
// dailySnapshotQuota: The quota threshold from DataMarket contract (pass 0 to count all slots with count > 0)
func (sc *SubmissionCounter) GetEligibleNodesCountForDay(dataMarket string, day string, dailySnapshotQuota int) int {
	// Try Redis first (source of truth)
	if redis.RedisClient != nil {
		eligibleNodesKey := redis.EligibleNodesByDayKey(dataMarket, day)
		slotIDs, err := redis.SMembers(sc.ctx, eligibleNodesKey)
		if err == nil {
			eligibleCount := 0
			for _, slotIDStr := range slotIDs {
				// Read count from EligibleSlotSubmissionKey (or SlotSubmissionKey as fallback)
				eligibleKey := redis.EligibleSlotSubmissionKey(dataMarket, slotIDStr, day)
				countStr, err := redis.Get(sc.ctx, eligibleKey)
				if err != nil || countStr == "" {
					// Fallback to SlotSubmissionKey
					slotKey := redis.SlotSubmissionKey(dataMarket, slotIDStr, day)
					countStr, err = redis.Get(sc.ctx, slotKey)
				}
				if err == nil && countStr != "" {
					if count, err := strconv.Atoi(countStr); err == nil {
						// Check if count meets quota threshold
						if dailySnapshotQuota == 0 {
							// If quota is 0, count all slots with submissions
							if count > 0 {
								eligibleCount++
							}
						} else {
							// Count slots that meet or exceed quota
							if count >= dailySnapshotQuota {
								eligibleCount++
							}
						}
					}
				}
			}
			return eligibleCount
		}
	}

	// Fallback to in-memory cache
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	count := 0
	if dayCounts, ok := sc.counts[dataMarket]; ok {
		if slotCounts, ok := dayCounts[day]; ok {
			for _, submissionCount := range slotCounts {
				if dailySnapshotQuota == 0 {
					if submissionCount > 0 {
						count++
					}
				} else {
					if submissionCount >= dailySnapshotQuota {
						count++
					}
				}
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

// ResetCountsForDay resets counts for a specific day (useful after final update)
func (sc *SubmissionCounter) ResetCountsForDay(dataMarket string, day string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if dayCounts, ok := sc.counts[dataMarket]; ok {
		delete(dayCounts, day)
		log.Printf("Reset counts for data market %s, day %s", dataMarket, day)
	}
}

// Helper function
func getTotalSlots(counts map[string]map[uint64]int) int {
	total := 0
	for _, slotCounts := range counts {
		total += len(slotCounts)
	}
	return total
}
