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
func (bp *BatchProcessor) ProcessValidatorBatch(vBatch *ValidatorBatch) error {
	// Extract data market using configured extractor
	// Note: Level 2 batches don't contain dataMarket info atm, so extractor returns configured value
	dataMarket := ""
	if bp.dataMarketExtractor != nil {
		// Create a temporary batch structure to extract data market
		// The extractor will return the configured data market address
		tempBatch := &FinalizedBatch{
			EpochId:           vBatch.EpochID,
			SubmissionDetails: make(map[string][]SubmissionMetadata),
		}
		dataMarket = bp.dataMarketExtractor(tempBatch)
	}

	// Check if we can accept batches for this epoch (must be past Level 1 delay)
	if bp.windowManager != nil && dataMarket != "" {
		if !bp.windowManager.CanAcceptBatch(vBatch.EpochID, dataMarket) {
			log.Printf("Skipping batch for epoch %d, dataMarket %s - Level 1 finalization delay not yet completed",
				vBatch.EpochID, dataMarket)
			return nil
		}
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Get or create epoch aggregation
	agg, exists := bp.epochAggregations[vBatch.EpochID]
	if !exists {
		agg = NewEpochAggregation(vBatch.EpochID)
		bp.epochAggregations[vBatch.EpochID] = agg
	}

	// Check if we already have this validator's batch for this epoch
	for _, existingBatch := range agg.Batches {
		if existingBatch.SequencerId == vBatch.ValidatorID {
			log.Printf("Already have batch from validator %s for epoch %d", vBatch.ValidatorID, vBatch.EpochID)
			return nil
		}
	}

	// Track first batch arrival
	isFirstBatch := len(agg.Batches) == 0

	// Create a placeholder FinalizedBatch from ValidatorBatch
	// In production, this would be fetched from IPFS or parsed from full message
	batch := &FinalizedBatch{
		EpochId:           vBatch.EpochID,
		SequencerId:       vBatch.ValidatorID,
		Timestamp:         vBatch.Timestamp,
		BatchIPFSCID:      vBatch.BatchIPFSCID,
		ProjectVotes:      make(map[string]uint32),
		SubmissionDetails: make(map[string][]SubmissionMetadata),
	}

	// Try to extract project votes from metadata if available
	if votes, ok := vBatch.Metadata["votes"].(map[string]interface{}); ok {
		for projectID, voteCount := range votes {
			if count, ok := voteCount.(float64); ok {
				batch.ProjectVotes[projectID] = uint32(count)
			}
		}
	}

	// Add batch to aggregation
	agg.Batches = append(agg.Batches, batch)
	agg.ReceivedBatches++
	agg.TotalValidators = len(agg.Batches)
	agg.UpdatedAt = time.Now()

	// Track validator contributions
	if projects, ok := vBatch.Metadata["projects"].(float64); ok {
		agg.ValidatorContributions[vBatch.ValidatorID] = []string{fmt.Sprintf("%.0f projects", projects)}
	}

	log.Printf("Processed batch from validator %s for epoch %d (total batches: %d)",
		vBatch.ValidatorID, vBatch.EpochID, agg.ReceivedBatches)

	// Notify window manager of first batch arrival
	if isFirstBatch && bp.windowManager != nil && dataMarket != "" {
		bp.windowManager.OnFirstBatchArrived(vBatch.EpochID, dataMarket)
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

	log.Printf("Aggregated epoch %d: %d projects, %d validators",
		epochID, len(aggregatedBatch.ProjectIds), agg.TotalValidators)

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

// ParseValidatorBatchMessage parses a JSON message into a ValidatorBatch
func ParseValidatorBatchMessage(data []byte) (*ValidatorBatch, error) {
	var vBatch ValidatorBatch
	if err := json.Unmarshal(data, &vBatch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal validator batch: %w", err)
	}

	vBatch.ReceivedAt = time.Now()
	return &vBatch, nil
}
