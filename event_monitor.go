package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EpochReleasedEvent represents an EpochReleased event from the protocol state contract
type EpochReleasedEvent struct {
	DataMarketAddress common.Address
	EpochID           *big.Int
	Begin             *big.Int
	End               *big.Int
	Timestamp         *big.Int
	BlockNumber       uint64
	BlockHash         common.Hash
}

// EventMonitor monitors EpochReleased events from the protocol state contract
type EventMonitor struct {
	ctx                context.Context
	client             *ethclient.Client
	protocolContract   common.Address
	dataMarketFilter   map[string]bool // Set of data market addresses to monitor (empty = all)
	eventCallback      func(*EpochReleasedEvent) error
	lastProcessedBlock uint64
	pollInterval       time.Duration
}

// NewEventMonitor creates a new event monitor
func NewEventMonitor(ctx context.Context, rpcURL string, protocolContract string, dataMarkets []string) (*EventMonitor, error) {
	if rpcURL == "" {
		return nil, fmt.Errorf("POWERLOOM_RPC_URL is required for event monitoring")
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %w", err)
	}

	protocolAddr := common.HexToAddress(protocolContract)
	if protocolAddr == (common.Address{}) {
		return nil, fmt.Errorf("invalid protocol state contract address: %s", protocolContract)
	}

	// Build data market filter
	dataMarketFilter := make(map[string]bool)
	for _, dm := range dataMarkets {
		if dm != "" {
			dataMarketFilter[strings.ToLower(dm)] = true
		}
	}

	pollInterval := 12 * time.Second // Default poll interval
	if intervalStr := os.Getenv("EVENT_POLL_INTERVAL"); intervalStr != "" {
		if interval, err := strconv.Atoi(intervalStr); err == nil {
			pollInterval = time.Duration(interval) * time.Second
		}
	}

	return &EventMonitor{
		ctx:                ctx,
		client:             client,
		protocolContract:   protocolAddr,
		dataMarketFilter:   dataMarketFilter,
		lastProcessedBlock: 0,
		pollInterval:       pollInterval,
	}, nil
}

// SetEventCallback sets the callback function to be called when EpochReleased events are detected
func (em *EventMonitor) SetEventCallback(callback func(*EpochReleasedEvent) error) {
	em.eventCallback = callback
}

// Start starts monitoring for EpochReleased events
func (em *EventMonitor) Start() error {
	// Get starting block
	startBlock := uint64(0)
	if startBlockStr := os.Getenv("EVENT_START_BLOCK"); startBlockStr != "" {
		if block, err := strconv.ParseUint(startBlockStr, 10, 64); err == nil {
			startBlock = block
		}
	}

	// If no start block specified, use latest block
	if startBlock == 0 {
		header, err := em.client.HeaderByNumber(em.ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to get latest block: %w", err)
		}
		startBlock = header.Number.Uint64()
		log.Printf("Starting event monitoring from latest block: %d", startBlock)
	} else {
		log.Printf("Starting event monitoring from block: %d", startBlock)
	}

	em.lastProcessedBlock = startBlock

	// Start polling loop
	go em.pollEvents()

	return nil
}

// pollEvents continuously polls for new events
func (em *EventMonitor) pollEvents() {
	ticker := time.NewTicker(em.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-em.ctx.Done():
			return
		case <-ticker.C:
			if err := em.processNewBlocks(); err != nil {
				log.Printf("Error processing blocks: %v", err)
			}
		}
	}
}

// processNewBlocks processes new blocks for EpochReleased events
func (em *EventMonitor) processNewBlocks() error {
	// Get latest block
	header, err := em.client.HeaderByNumber(em.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	latestBlock := header.Number.Uint64()

	// Process blocks in batches if we're behind
	batchSize := uint64(1000)
	if batchSizeStr := os.Getenv("EVENT_BLOCK_BATCH_SIZE"); batchSizeStr != "" {
		if bs, err := strconv.ParseUint(batchSizeStr, 10, 64); err == nil {
			batchSize = bs
		}
	}

	fromBlock := em.lastProcessedBlock + 1
	if fromBlock > latestBlock {
		return nil // No new blocks
	}

	toBlock := fromBlock + batchSize - 1
	if toBlock > latestBlock {
		toBlock = latestBlock
	}

	// Query for EpochReleased events
	// Event signature: EpochReleased(address indexed dataMarketAddress, uint256 indexed epochId, uint256 begin, uint256 end, uint256 timestamp)
	// Event ID: keccak256("EpochReleased(address,uint256,uint256,uint256,uint256)")
	// This is: 0xf7d2257d4a1c445138ab52bd3c22425bfed29da81d0173961c697dc14fcba60c
	eventSignature := common.HexToHash("0xf7d2257d4a1c445138ab52bd3c22425bfed29da81d0173961c697dc14fcba60c")

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: []common.Address{em.protocolContract},
		Topics: [][]common.Hash{
			{eventSignature}, // Event signature
		},
	}

	logs, err := em.client.FilterLogs(em.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %w", err)
	}

	log.Printf("Found %d EpochReleased events in blocks %d-%d", len(logs), fromBlock, toBlock)

	// Process each event
	for _, vLog := range logs {
		event, err := em.parseEpochReleasedEvent(vLog)
		if err != nil {
			log.Printf("Failed to parse EpochReleased event: %v", err)
			continue
		}

		// Filter by data market if filter is set
		if len(em.dataMarketFilter) > 0 {
			dataMarketLower := strings.ToLower(event.DataMarketAddress.Hex())
			if !em.dataMarketFilter[dataMarketLower] {
				log.Printf("Skipping EpochReleased event for data market %s (not in filter)", event.DataMarketAddress.Hex())
				continue
			}
		}

		log.Printf("📅 EpochReleased: epoch=%s, dataMarket=%s, block=%d",
			event.EpochID.String(), event.DataMarketAddress.Hex(), event.BlockNumber)

		// Call callback if set
		if em.eventCallback != nil {
			if err := em.eventCallback(event); err != nil {
				log.Printf("Error in EpochReleased callback: %v", err)
			}
		}
	}

	em.lastProcessedBlock = toBlock
	return nil
}

// parseEpochReleasedEvent parses an EpochReleased event from a log
// Event structure: EpochReleased(address indexed dataMarketAddress, uint256 indexed epochId, uint256 begin, uint256 end, uint256 timestamp)
// Topics[0] = event signature
// Topics[1] = dataMarketAddress (indexed)
// Topics[2] = epochId (indexed)
// Data = abi.encode(begin, end, timestamp)
func (em *EventMonitor) parseEpochReleasedEvent(vLog types.Log) (*EpochReleasedEvent, error) {
	if len(vLog.Topics) < 3 {
		return nil, fmt.Errorf("invalid EpochReleased event: expected at least 3 topics, got %d", len(vLog.Topics))
	}

	// Extract indexed parameters
	dataMarketAddress := common.BytesToAddress(vLog.Topics[1].Bytes())
	epochID := new(big.Int).SetBytes(vLog.Topics[2].Bytes())

	// Extract non-indexed parameters from data
	// Data contains: begin (uint256), end (uint256), timestamp (uint256)
	// Each uint256 is 32 bytes
	if len(vLog.Data) < 96 {
		return nil, fmt.Errorf("invalid EpochReleased event data: expected at least 96 bytes, got %d", len(vLog.Data))
	}

	begin := new(big.Int).SetBytes(vLog.Data[0:32])
	end := new(big.Int).SetBytes(vLog.Data[32:64])
	timestamp := new(big.Int).SetBytes(vLog.Data[64:96])

	return &EpochReleasedEvent{
		DataMarketAddress: dataMarketAddress,
		EpochID:           epochID,
		Begin:             begin,
		End:               end,
		Timestamp:         timestamp,
		BlockNumber:       vLog.BlockNumber,
		BlockHash:         vLog.BlockHash,
	}, nil
}

// Close closes the event monitor
func (em *EventMonitor) Close() {
	if em.client != nil {
		em.client.Close()
	}
}
