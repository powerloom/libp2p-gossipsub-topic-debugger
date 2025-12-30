package main

import (
	"fmt"
	"log"
)

// ApplyConsensus applies majority vote logic to aggregate batches per epoch
func ApplyConsensus(batches []*FinalizedBatch) *FinalizedBatch {
	if len(batches) == 0 {
		return nil
	}

	// Use the first batch as base
	baseBatch := batches[0]
	aggregated := &FinalizedBatch{
		EpochId:           baseBatch.EpochId,
		ProjectIds:        make([]string, 0),
		SnapshotCids:      make([]string, 0),
		MerkleRoot:        baseBatch.MerkleRoot,
		BlsSignature:      baseBatch.BlsSignature,
		SequencerId:       "aggregated",
		Timestamp:          baseBatch.Timestamp,
		ProjectVotes:      make(map[string]uint32),
		SubmissionDetails: make(map[string][]SubmissionMetadata),
		ValidatorCount:    len(batches),
	}

	// Collect all unique projects and their votes
	projectVoteMap := make(map[string]map[string]uint32) // projectID -> CID -> vote count
	projectCIDMap := make(map[string]string)             // projectID -> winning CID

	// Aggregate votes from all batches
	for _, batch := range batches {
		// Process ProjectVotes
		for projectID, voteCount := range batch.ProjectVotes {
			if projectVoteMap[projectID] == nil {
				projectVoteMap[projectID] = make(map[string]uint32)
			}

			// Find corresponding CID for this project
			var cid string
			for i, pid := range batch.ProjectIds {
				if pid == projectID && i < len(batch.SnapshotCids) {
					cid = batch.SnapshotCids[i]
					break
				}
			}

			if cid != "" {
				projectVoteMap[projectID][cid] += voteCount
			}
		}

		// Merge SubmissionDetails
		for projectID, submissions := range batch.SubmissionDetails {
			if aggregated.SubmissionDetails[projectID] == nil {
				aggregated.SubmissionDetails[projectID] = make([]SubmissionMetadata, 0)
			}
			aggregated.SubmissionDetails[projectID] = append(
				aggregated.SubmissionDetails[projectID],
				submissions...,
			)
		}
	}

	// Select winning CID for each project (majority vote)
	for projectID, cidVotes := range projectVoteMap {
		winningCID := SelectWinningCID(projectID, cidVotes)
		if winningCID != "" {
			projectCIDMap[projectID] = winningCID
		}
	}

	// Build aggregated arrays
	for projectID, cid := range projectCIDMap {
		aggregated.ProjectIds = append(aggregated.ProjectIds, projectID)
		aggregated.SnapshotCids = append(aggregated.SnapshotCids, cid)

		// Aggregate votes for this project
		if votes, ok := projectVoteMap[projectID]; ok {
			var totalVotes uint32
			for _, count := range votes {
				totalVotes += count
			}
			aggregated.ProjectVotes[projectID] = totalVotes
		}

		// Keep only submissions for winning CID and aggregate vote counts across validators
		if submissions, ok := aggregated.SubmissionDetails[projectID]; ok {
			// Track unique submissions by slotID+CID and aggregate their vote counts
			// Key: "slotID:cid" -> aggregated submission metadata
			submissionMap := make(map[string]*SubmissionMetadata)
			// Track which validators reported each submission
			validatorSet := make(map[string]map[string]bool) // key: "slotID:cid" -> set of validator IDs
			
			for _, sub := range submissions {
				if sub.SnapshotCID == cid {
					key := fmt.Sprintf("%d:%s", sub.SlotID, sub.SnapshotCID)
					
					// Initialize validator set for this key if needed
					if validatorSet[key] == nil {
						validatorSet[key] = make(map[string]bool)
					}
					
					// Add all validators that confirmed this submission
					for _, v := range sub.ValidatorsConfirming {
						validatorSet[key][v] = true
					}
					
					// If we've seen this submission before, update the existing entry
					if existing, exists := submissionMap[key]; exists {
						// Update vote count to total unique validators
						existing.VoteCount = len(validatorSet[key])
						// Update validators list
						existing.ValidatorsConfirming = make([]string, 0, len(validatorSet[key]))
						for v := range validatorSet[key] {
							existing.ValidatorsConfirming = append(existing.ValidatorsConfirming, v)
						}
					} else {
						// First time seeing this submission - create new entry
						submissionMap[key] = &SubmissionMetadata{
							SubmitterID:          sub.SubmitterID,
							SnapshotCID:          sub.SnapshotCID,
							Timestamp:            sub.Timestamp,
							Signature:            sub.Signature,
							SlotID:               sub.SlotID,
							VoteCount:            len(validatorSet[key]), // Count unique validators
							ValidatorsConfirming: make([]string, 0, len(validatorSet[key])),
						}
						// Add all validators to the list
						for v := range validatorSet[key] {
							submissionMap[key].ValidatorsConfirming = append(submissionMap[key].ValidatorsConfirming, v)
						}
					}
				}
			}
			
			// Convert map to slice
			filtered := make([]SubmissionMetadata, 0, len(submissionMap))
			for _, sub := range submissionMap {
				filtered = append(filtered, *sub)
			}
			aggregated.SubmissionDetails[projectID] = filtered
		}
	}

	log.Printf("Consensus applied: %d projects aggregated from %d validator batches",
		len(aggregated.ProjectIds), len(batches))

	return aggregated
}

// SelectWinningCID selects the CID with the most votes for a project
func SelectWinningCID(projectID string, votes map[string]uint32) string {
	if len(votes) == 0 {
		return ""
	}

	var winningCID string
	var maxVotes uint32

	for cid, voteCount := range votes {
		if voteCount > maxVotes {
			maxVotes = voteCount
			winningCID = cid
		}
	}

	if winningCID == "" {
		log.Printf("Warning: No winning CID found for project %s", projectID)
		return ""
	}

	log.Printf("Project %s: selected CID %s with %d votes", projectID, winningCID, maxVotes)
	return winningCID
}

