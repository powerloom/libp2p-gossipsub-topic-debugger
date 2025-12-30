package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TallyDump represents a per-epoch tally dump
type TallyDump struct {
	EpochID            uint64            `json:"epoch_id"`
	DataMarket         string            `json:"data_market"`
	Timestamp          int64             `json:"timestamp"`
	SubmissionCounts   map[string]int    `json:"submission_counts"` // slotID -> count
	EligibleNodesCount int               `json:"eligible_nodes_count"`
	TotalValidators    int               `json:"total_validators"`
	AggregatedProjects map[string]string `json:"aggregated_projects"`  // projectID -> winning CID
	ValidatorBatchCIDs map[string]string `json:"validator_batch_cids"` // validatorID -> batch_ipfs_cid
}

// TallyDumper handles generating and managing per-epoch tally dumps
type TallyDumper struct {
	dumpDir            string
	retentionFiles     int
	retentionDays      int
	pruneIntervalHours int
	enabled            bool
	mu                 sync.Mutex
}

// NewTallyDumper creates a new tally dumper
func NewTallyDumper() *TallyDumper {
	dumpDir := os.Getenv("TALLY_DUMP_DIR")
	if dumpDir == "" {
		dumpDir = "./tallies"
	}

	retentionFiles := 1000 // Default
	if filesStr := os.Getenv("TALLY_RETENTION_FILES"); filesStr != "" {
		if files, err := strconv.Atoi(filesStr); err == nil {
			retentionFiles = files
		}
	}

	retentionDays := 7 // Default
	if daysStr := os.Getenv("TALLY_RETENTION_DAYS"); daysStr != "" {
		if days, err := strconv.Atoi(daysStr); err == nil {
			retentionDays = days
		}
	}

	pruneInterval := 1 // Default 1 hour
	if intervalStr := os.Getenv("TALLY_PRUNE_INTERVAL_HOURS"); intervalStr != "" {
		if interval, err := strconv.Atoi(intervalStr); err == nil {
			pruneInterval = interval
		}
	}

	enabled := os.Getenv("ENABLE_TALLY_DUMPS") != "false" // Default true

	return &TallyDumper{
		dumpDir:            dumpDir,
		retentionFiles:     retentionFiles,
		retentionDays:      retentionDays,
		pruneIntervalHours: pruneInterval,
		enabled:            enabled,
	}
}

// Initialize creates the dump directory and starts pruning routine
func (td *TallyDumper) Initialize(ctx context.Context) error {
	if !td.enabled {
		log.Printf("Tally dumps disabled (ENABLE_TALLY_DUMPS=false)")
		return nil
	}

	// Create dump directory
	if err := os.MkdirAll(td.dumpDir, 0755); err != nil {
		return fmt.Errorf("failed to create tally dump directory: %w", err)
	}

	log.Printf("Tally dumps enabled: dir=%s, retentionFiles=%d, retentionDays=%d",
		td.dumpDir, td.retentionFiles, td.retentionDays)

	// Prune on startup
	if err := td.Prune(); err != nil {
		log.Printf("Warning: Failed to prune on startup: %v", err)
	}

	// Start periodic pruning
	go td.periodicPrune(ctx)

	return nil
}

// Dump generates a tally dump for an epoch
func (td *TallyDumper) Dump(epochID uint64, dataMarket string, counts map[uint64]int, eligibleNodesCount int, totalValidators int, aggregatedProjects map[string]string, validatorBatchCIDs map[string]string) error {
	if !td.enabled {
		return nil
	}

	td.mu.Lock()
	defer td.mu.Unlock()

	// Convert counts to string keys for JSON
	submissionCounts := make(map[string]int)
	for slotID, count := range counts {
		submissionCounts[strconv.FormatUint(slotID, 10)] = count
	}

	tally := &TallyDump{
		EpochID:            epochID,
		DataMarket:         dataMarket,
		Timestamp:          time.Now().Unix(),
		SubmissionCounts:   submissionCounts,
		EligibleNodesCount: eligibleNodesCount,
		TotalValidators:    totalValidators,
		AggregatedProjects: aggregatedProjects,
		ValidatorBatchCIDs: validatorBatchCIDs,
	}

	// Marshal JSON for file output
	jsonBytes, err := json.MarshalIndent(tally, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tally: %w", err)
	}

	// Write to file
	filename := fmt.Sprintf("epoch_%d_%s.json", epochID, strings.ToLower(strings.TrimPrefix(dataMarket, "0x")))
	filepath := filepath.Join(td.dumpDir, filename)

	if err := os.WriteFile(filepath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("failed to write tally file: %w", err)
	}

	// Log summary instead of full JSON
	validatorInfo := ""
	if len(validatorBatchCIDs) > 0 {
		validatorList := make([]string, 0, len(validatorBatchCIDs))
		for validatorID, batchCID := range validatorBatchCIDs {
			validatorList = append(validatorList, fmt.Sprintf("%s:%s", validatorID, batchCID))
		}
		validatorInfo = fmt.Sprintf(", validatorBatches=%v", validatorList)
	}
	log.Printf("📊 TALLY DUMP: epoch=%d, dataMarket=%s, slots=%d, eligibleNodes=%d, validators=%d%s → %s",
		epochID, dataMarket, len(counts), eligibleNodesCount, totalValidators, validatorInfo, filepath)

	return nil
}

// Prune removes old tally files based on retention policies
func (td *TallyDumper) Prune() error {
	if !td.enabled {
		return nil
	}

	entries, err := os.ReadDir(td.dumpDir)
	if err != nil {
		return fmt.Errorf("failed to read dump directory: %w", err)
	}

	// Collect file info
	type fileInfo struct {
		path    string
		modTime time.Time
	}

	files := make([]fileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, fileInfo{
			path:    filepath.Join(td.dumpDir, entry.Name()),
			modTime: info.ModTime(),
		})
	}

	// Sort by modification time (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	cutoffTime := time.Now().Add(-time.Duration(td.retentionDays) * 24 * time.Hour)
	deletedCount := 0

	// Apply retention policies
	for i, file := range files {
		shouldDelete := false

		// Check file count retention
		if td.retentionFiles > 0 && len(files)-i > td.retentionFiles {
			shouldDelete = true
		}

		// Check time-based retention
		if td.retentionDays > 0 && file.modTime.Before(cutoffTime) {
			shouldDelete = true
		}

		if shouldDelete {
			if err := os.Remove(file.path); err != nil {
				log.Printf("Warning: Failed to delete old tally file %s: %v", file.path, err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		log.Printf("Pruned %d old tally files (retention: %d files, %d days)", deletedCount, td.retentionFiles, td.retentionDays)
	}

	return nil
}

// periodicPrune runs pruning periodically
func (td *TallyDumper) periodicPrune(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(td.pruneIntervalHours) * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := td.Prune(); err != nil {
				log.Printf("Error during periodic prune: %v", err)
			}
		}
	}
}
