package main

import (
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

		// Keep only submissions for winning CID
		if submissions, ok := aggregated.SubmissionDetails[projectID]; ok {
			filtered := make([]SubmissionMetadata, 0)
			for _, sub := range submissions {
				if sub.SnapshotCID == cid {
					filtered = append(filtered, sub)
				}
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

