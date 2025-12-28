package main

import (
	"time"
)

// SubmissionMetadata tracks WHO submitted WHAT for challenges/proofs
// This provides accountability and enables challenge workflows in the protocol
type SubmissionMetadata struct {
	SubmitterID          string   `json:"submitter_id"`            // Snapshotter's EVM address
	SnapshotCID          string   `json:"snapshot_cid"`            // IPFS CID of the snapshot data
	Timestamp            uint64   `json:"timestamp"`               // Unix timestamp when submission was finalized
	Signature            []byte   `json:"signature"`               // Snapshotter's EIP-712 signature
	SlotID               uint64   `json:"slot_id"`                 // Numeric slot ID of the snapshotter
	VoteCount            int      `json:"vote_count"`             // How many validators confirmed seeing this submission
	ValidatorsConfirming []string `json:"validators_confirming"`  // List of validator IDs that saw and confirmed this submission
}

// FinalizedBatch represents a validator's finalized batch for an epoch
type FinalizedBatch struct {
	EpochId           uint64                        `json:"EpochId"`
	ProjectIds        []string                      `json:"ProjectIds"`
	SnapshotCids      []string                      `json:"SnapshotCids"`
	MerkleRoot        []byte                        `json:"MerkleRoot"`
	BlsSignature      []byte                        `json:"BlsSignature"`
	SequencerId       string                        `json:"SequencerId"`
	Timestamp         uint64                        `json:"Timestamp"`
	ProjectVotes      map[string]uint32             `json:"ProjectVotes"`       // project → vote count
	SubmissionDetails map[string][]SubmissionMetadata `json:"submission_details"` // project → submissions mapping
	ValidatorBatches  map[string]string              `json:"ValidatorBatches,omitempty"`   // validator_id → ipfs_cid
	BatchIPFSCID      string                        `json:"batch_ipfs_cid,omitempty"`      // IPFS CID of this aggregated batch
	ValidatorCount    int                           `json:"ValidatorCount,omitempty"`       // Number of validators who contributed
}

// ValidatorBatch represents a batch message from a validator
type ValidatorBatch struct {
	ValidatorID  string    `json:"validator_id"`
	PeerID       string    `json:"peer_id"`
	EpochID      uint64    `json:"epoch_id"`
	BatchIPFSCID string    `json:"batch_ipfs_cid"`
	MerkleRoot   string    `json:"merkle_root"`
	ProjectCount int       `json:"project_count"`
	Timestamp    uint64    `json:"timestamp"`
	Signature    string    `json:"signature"`
	ReceivedAt   time.Time `json:"received_at"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// EpochAggregation tracks the aggregated state of all validator batches for an epoch
type EpochAggregation struct {
	EpochID               uint64                       `json:"epoch_id"`
	TotalValidators       int                          `json:"total_validators"`
	ReceivedBatches       int                          `json:"received_batches"`
	Batches               []*FinalizedBatch            `json:"batches"`                    // All batches received
	ProjectVotes          map[string]map[string]int     `json:"project_votes"`             // projectID -> CID -> vote count
	AggregatedProjects    map[string]string            `json:"aggregated_projects"`       // projectID -> winning CID
	AggregatedBatch       *FinalizedBatch              `json:"aggregated_batch,omitempty"` // Final consensus batch
	ValidatorContributions map[string][]string         `json:"validator_contributions"`   // validatorID -> list of projects
	UpdatedAt             time.Time                    `json:"updated_at"`
}

// NewEpochAggregation creates a new epoch aggregation tracker
func NewEpochAggregation(epochID uint64) *EpochAggregation {
	return &EpochAggregation{
		EpochID:               epochID,
		Batches:               make([]*FinalizedBatch, 0),
		ProjectVotes:          make(map[string]map[string]int),
		AggregatedProjects:    make(map[string]string),
		ValidatorContributions: make(map[string][]string),
		UpdatedAt:             time.Now(),
	}
}

