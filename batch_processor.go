package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// BatchProcessor handles processing and aggregation of validator batches
type BatchProcessor struct {
	ctx                 context.Context
	epochAggregations   map[uint64]*EpochAggregation
	mu                  sync.RWMutex
	updateCallback      func(epochID uint64, batch *FinalizedBatch) error
	windowManager       *WindowManager
	dataMarketExtractor func(*FinalizedBatch) string // Extract data market from batch
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(ctx context.Context, windowManager *WindowManager) *BatchProcessor {
	return &BatchProcessor{
		ctx:               ctx,
		epochAggregations: make(map[uint64]*EpochAggregation),
		windowManager:     windowManager,
	}
}

// SetDataMarketExtractor sets a function to extract data market address from a batch
func (bp *BatchProcessor) SetDataMarketExtractor(extractor func(*FinalizedBatch) string) {
	bp.dataMarketExtractor = extractor
}

// SetUpdateCallback sets a callback function to be called when an epoch is aggregated
func (bp *BatchProcessor) SetUpdateCallback(callback func(epochID uint64, batch *FinalizedBatch) error) {
	bp.updateCallback = callback
}

// ProcessValidatorBatch processes an incoming validator batch message
func (bp *BatchProcessor) ProcessValidatorBatch(batch *FinalizedBatch) error {
	// Extract data market using configured extractor
	// Note: Level 2 batches don't contain dataMarket info atm, so extractor returns configured value
	dataMarket := ""
	if bp.dataMarketExtractor != nil {
		dataMarket = bp.dataMarketExtractor(batch)
	}

	// Check if we can accept batches for this epoch (must be past Level 1 delay)
	if bp.windowManager != nil && dataMarket != "" {
		if !bp.windowManager.CanAcceptBatch(batch.EpochId, dataMarket) {
			log.Printf("⏸️  Skipping batch for epoch %d, dataMarket %s - Level 1 finalization delay not yet completed",
				batch.EpochId, dataMarket)
			return nil
		}
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Get or create epoch aggregation
	agg, exists := bp.epochAggregations[batch.EpochId]
	if !exists {
		agg = NewEpochAggregation(batch.EpochId)
		bp.epochAggregations[batch.EpochId] = agg
		log.Printf("📦 Created new aggregation for epoch %d", batch.EpochId)
	}

	// Check if we already have this validator's batch for this epoch
	for _, existingBatch := range agg.Batches {
		if existingBatch.SequencerId == batch.SequencerId {
			log.Printf("⚠️  Already have batch from validator %s for epoch %d", batch.SequencerId, batch.EpochId)
			return nil
		}
	}

	// Track first batch arrival
	isFirstBatch := len(agg.Batches) == 0

	// Count submissions for logging
	submissionCount := 0
	for _, submissions := range batch.SubmissionDetails {
		submissionCount += len(submissions)
	}

	// Add batch to aggregation
	agg.Batches = append(agg.Batches, batch)
	agg.ReceivedBatches++
	agg.TotalValidators = len(agg.Batches)
	agg.UpdatedAt = time.Now()

	// Track validator contributions
	projectList := make([]string, 0, len(batch.ProjectIds))
	for _, pid := range batch.ProjectIds {
		projectList = append(projectList, pid)
	}
	agg.ValidatorContributions[batch.SequencerId] = projectList

	log.Printf("✅ Processed batch: epoch=%d, validator=%s, projects=%d, submissions=%d (total batches: %d)",
		batch.EpochId, batch.SequencerId, len(batch.ProjectIds), submissionCount, agg.ReceivedBatches)

	// Notify window manager of first batch arrival
	if isFirstBatch && bp.windowManager != nil && dataMarket != "" {
		bp.windowManager.OnFirstBatchArrived(batch.EpochId, dataMarket)
	}

	// Don't aggregate immediately - wait for window to close
	// Aggregation will be triggered by window manager callback

	return nil
}

// ProcessFullBatch processes a complete FinalizedBatch (e.g., fetched from IPFS)
func (bp *BatchProcessor) ProcessFullBatch(batch *FinalizedBatch) error {
	// Extract data market using configured extractor
	// Note: Level 2 batches don't contain dataMarket info, so extractor returns configured value
	dataMarket := ""
	if bp.dataMarketExtractor != nil {
		dataMarket = bp.dataMarketExtractor(batch)
	}

	// Check if we can accept batches for this epoch (must be past Level 1 delay)
	if bp.windowManager != nil && dataMarket != "" {
		if !bp.windowManager.CanAcceptBatch(batch.EpochId, dataMarket) {
			log.Printf("Skipping batch for epoch %d, dataMarket %s - Level 1 finalization delay not yet completed",
				batch.EpochId, dataMarket)
			return nil
		}
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Get or create epoch aggregation
	agg, exists := bp.epochAggregations[batch.EpochId]
	if !exists {
		agg = NewEpochAggregation(batch.EpochId)
		bp.epochAggregations[batch.EpochId] = agg
	}

	// Check if we already have this validator's batch
	for _, existingBatch := range agg.Batches {
		if existingBatch.SequencerId == batch.SequencerId {
			log.Printf("Already have batch from validator %s for epoch %d", batch.SequencerId, batch.EpochId)
			return nil
		}
	}

	// Track first batch arrival
	isFirstBatch := len(agg.Batches) == 0

	// Add batch to aggregation
	agg.Batches = append(agg.Batches, batch)
	agg.ReceivedBatches++
	agg.TotalValidators = len(agg.Batches)
	agg.UpdatedAt = time.Now()

	log.Printf("Processed full batch from validator %s for epoch %d (total batches: %d)",
		batch.SequencerId, batch.EpochId, agg.ReceivedBatches)

	// Notify window manager of first batch arrival
	if isFirstBatch && bp.windowManager != nil && dataMarket != "" {
		bp.windowManager.OnFirstBatchArrived(batch.EpochId, dataMarket)
	}

	// Don't aggregate immediately - wait for window to close
	// Aggregation will be triggered by window manager callback

	return nil
}

// aggregateEpoch applies consensus logic to aggregate batches for an epoch
// This should only be called when the aggregation window has closed
func (bp *BatchProcessor) aggregateEpoch(epochID uint64, dataMarket string) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	agg, exists := bp.epochAggregations[epochID]
	if !exists || len(agg.Batches) == 0 {
		return fmt.Errorf("no batches found for epoch %d", epochID)
	}

	// Verify window is closed before aggregating
	if bp.windowManager != nil && dataMarket != "" {
		if !bp.windowManager.IsWindowClosed(epochID, dataMarket) {
			log.Printf("Skipping aggregation for epoch %d - window not yet closed", epochID)
			return nil
		}
	}

	// Aggregate project votes across all batches
	projectVoteMap := make(map[string]map[string]int) // projectID -> CID -> count

	for _, batch := range agg.Batches {
		// Aggregate votes per project
		for projectID, voteCount := range batch.ProjectVotes {
			if projectVoteMap[projectID] == nil {
				projectVoteMap[projectID] = make(map[string]int)
			}
			// Use the CID from SnapshotCids array if available
			// For now, we'll use projectID as key since we don't have CID mapping yet
			projectVoteMap[projectID][projectID] += int(voteCount)
		}
	}

	agg.ProjectVotes = projectVoteMap

	// Apply consensus to select winning CIDs per project
	aggregatedBatch := ApplyConsensus(agg.Batches)
	agg.AggregatedBatch = aggregatedBatch
	agg.AggregatedProjects = make(map[string]string)

	// Extract winning projects
	for i, projectID := range aggregatedBatch.ProjectIds {
		if i < len(aggregatedBatch.SnapshotCids) {
			agg.AggregatedProjects[projectID] = aggregatedBatch.SnapshotCids[i]
		}
	}

	log.Printf("✅ Aggregated epoch %d: %d projects, %d validators, %d total submissions",
		epochID, len(aggregatedBatch.ProjectIds), agg.TotalValidators, len(aggregatedBatch.SubmissionDetails))

	// Call update callback if set
	if bp.updateCallback != nil {
		if err := bp.updateCallback(epochID, aggregatedBatch); err != nil {
			log.Printf("Error in update callback for epoch %d: %v", epochID, err)
			return err
		}
	}

	return nil
}

// GetEpochAggregation returns the aggregation state for an epoch
func (bp *BatchProcessor) GetEpochAggregation(epochID uint64) *EpochAggregation {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.epochAggregations[epochID]
}

// ParseValidatorBatchMessage parses a JSON message into a FinalizedBatch
// The JSON structure matches FinalizedBatch, not ValidatorBatch
func ParseValidatorBatchMessage(data []byte) (*FinalizedBatch, error) {
	var batch FinalizedBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal validator batch: %w", err)
	}

	// Ensure maps are initialized
	if batch.ProjectVotes == nil {
		batch.ProjectVotes = make(map[string]uint32)
	}
	if batch.SubmissionDetails == nil {
		batch.SubmissionDetails = make(map[string][]SubmissionMetadata)
	}

	return &batch, nil
}
