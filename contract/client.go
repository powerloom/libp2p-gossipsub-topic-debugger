package contract

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	rpchelper "github.com/powerloom/go-rpc-helper"
)

// Client handles contract interactions
type Client struct {
	rpcHelper           *rpchelper.RPCHelper
	contractBackend     *rpchelper.ContractBackend
	protocolContract    common.Address
	dataMarketABI       string // DataMarket ABI JSON string
	updateMethod        string // "direct" or "relayer"
	relayerURL          string
	relayerAuthToken    string
	evmPrivateKey       string // EVM private key for direct contract calls (secp256k1)
	updateEpochInterval int64
	batchSize           int           // Batch size for splitting slot IDs and submission counts
	step1ToStep2Delay   time.Duration // Delay between Step 1 (updateEligibleNodes) and Step 2 (updateEligibleSubmissionCounts)
}

// NewClient creates a new contract client
func NewClient() (*Client, error) {
	// Check if contract updates are enabled
	enabled := os.Getenv("ENABLE_CONTRACT_UPDATES") == "true"
	if !enabled {
		return &Client{
			updateMethod:        "disabled",
			updateEpochInterval: 0,
			batchSize:           50, // Default even when disabled
		}, nil
	}

	updateMethod := os.Getenv("CONTRACT_UPDATE_METHOD")
	if updateMethod == "" {
		updateMethod = "relayer" // Default to relayer
	}

	// Support both POWERLOOM_RPC_NODES (comma-separated) and POWERLOOM_RPC_URL (single, for backward compatibility)
	rpcNodesStr := os.Getenv("POWERLOOM_RPC_NODES")
	if rpcNodesStr == "" {
		rpcNodesStr = os.Getenv("POWERLOOM_RPC_URL") // Fallback to single URL
	}

	var rpcURLs []string
	if rpcNodesStr != "" {
		// Parse comma-separated list
		for _, url := range strings.Split(rpcNodesStr, ",") {
			url = strings.TrimSpace(url)
			if url != "" {
				rpcURLs = append(rpcURLs, url)
			}
		}
	}

	relayerURL := os.Getenv("RELAYER_URL")
	relayerAuthToken := os.Getenv("RELAYER_AUTH_TOKEN")
	evmPrivateKey := os.Getenv("EVM_PRIVATE_KEY") // EVM private key for direct contract calls

	protocolContractStr := os.Getenv("PROTOCOL_STATE_CONTRACT")
	if protocolContractStr == "" {
		return nil, fmt.Errorf("PROTOCOL_STATE_CONTRACT environment variable is required")
	}
	protocolContract := common.HexToAddress(protocolContractStr)

	// Load DataMarket ABI for day fetching
	dataMarketABI, err := loadDataMarketABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load DataMarket ABI: %w", err)
	}

	updateInterval := int64(10) // Default 10 epochs
	if intervalStr := os.Getenv("SUBMISSION_UPDATE_EPOCH_INTERVAL"); intervalStr != "" {
		if interval, err := strconv.ParseInt(intervalStr, 10, 64); err == nil {
			updateInterval = interval
		}
	}

	batchSize := 50 // Default batch size (EVM-safe limit)
	if batchSizeStr := os.Getenv("REWARDS_UPDATE_BATCH_SIZE"); batchSizeStr != "" {
		if bs, err := strconv.Atoi(batchSizeStr); err == nil && bs > 0 {
			batchSize = bs
		}
	}

	// Delay between Step 1 (updateEligibleNodes) and Step 2 (updateEligibleSubmissionCounts)
	// Default: 3 seconds to allow Step 1 transaction to be mined
	step1ToStep2Delay := 3 * time.Second
	if delayStr := os.Getenv("STEP1_TO_STEP2_DELAY_SECONDS"); delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil && delay >= 0 {
			step1ToStep2Delay = time.Duration(delay) * time.Second
		}
	}

	client := &Client{
		protocolContract:    protocolContract,
		dataMarketABI:       dataMarketABI,
		updateMethod:        updateMethod,
		relayerURL:          relayerURL,
		relayerAuthToken:    relayerAuthToken,
		evmPrivateKey:       evmPrivateKey,
		updateEpochInterval: updateInterval,
		batchSize:           batchSize,
		step1ToStep2Delay:   step1ToStep2Delay,
	}

	// Initialize RPC helper with multiple nodes (needed for day fetching even with relayer method)
	if len(rpcURLs) > 0 {
		// Build RPC config similar to protocl-state-cacher
		rpcConfig := &rpchelper.RPCConfig{
			Nodes: func() []rpchelper.NodeConfig {
				var nodes []rpchelper.NodeConfig
				for _, url := range rpcURLs {
					nodes = append(nodes, rpchelper.NodeConfig{URL: url})
				}
				return nodes
			}(),
			MaxRetries:     5,
			RetryDelay:     200 * time.Millisecond,
			MaxRetryDelay:  5 * time.Second,
			RequestTimeout: 30 * time.Second,
		}

		client.rpcHelper = rpchelper.NewRPCHelper(rpcConfig)

		// Initialize the RPC helper
		initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := client.rpcHelper.Initialize(initCtx); err != nil {
			return nil, fmt.Errorf("failed to initialize RPC helper: %w", err)
		}

		// Create ContractBackend that uses the RPC helper for all contract calls
		client.contractBackend = client.rpcHelper.NewContractBackend()
	}

	// Initialize ethclient if using direct method
	if updateMethod == "direct" {
		if len(rpcURLs) == 0 {
			return nil, fmt.Errorf("POWERLOOM_RPC_NODES or POWERLOOM_RPC_URL is required when CONTRACT_UPDATE_METHOD=direct")
		}
		if evmPrivateKey == "" {
			return nil, fmt.Errorf("EVM_PRIVATE_KEY is required when CONTRACT_UPDATE_METHOD=direct")
		}
	}

	return client, nil
}

// loadDataMarketABI loads the DataMarket ABI from the embedded file
func loadDataMarketABI() (string, error) {
	// Read ABI file
	abiPath := "contract/abi/DataMarket.json"
	if _, err := os.Stat(abiPath); os.IsNotExist(err) {
		// Try alternative path
		abiPath = "./contract/abi/DataMarket.json"
		if _, err := os.Stat(abiPath); os.IsNotExist(err) {
			return "", fmt.Errorf("DataMarket ABI file not found at contract/abi/DataMarket.json")
		}
	}

	abiBytes, err := os.ReadFile(abiPath)
	if err != nil {
		return "", fmt.Errorf("failed to read DataMarket ABI file: %w", err)
	}

	// Parse JSON to extract ABI array
	var artifact struct {
		ABI json.RawMessage `json:"abi"`
	}
	if err := json.Unmarshal(abiBytes, &artifact); err != nil {
		return "", fmt.Errorf("failed to parse DataMarket ABI JSON: %w", err)
	}

	return string(artifact.ABI), nil
}

// FetchCurrentDay fetches the current day for a data market from the DataMarket contract
// Uses RPC helper with automatic failover across multiple nodes
func (c *Client) FetchCurrentDay(ctx context.Context, dataMarketAddress common.Address) (*big.Int, error) {
	log.Printf("🔍 FetchCurrentDay: Calling dayCounter() on DataMarket contract %s", dataMarketAddress.Hex())

	if c.contractBackend == nil {
		log.Printf("❌ FetchCurrentDay: RPC helper not initialized")
		return nil, fmt.Errorf("RPC helper not initialized - POWERLOOM_RPC_NODES or POWERLOOM_RPC_URL is required for day fetching")
	}

	// Parse ABI
	parsedABI, err := parseABI(c.dataMarketABI)
	if err != nil {
		log.Printf("❌ FetchCurrentDay: Failed to parse DataMarket ABI: %v", err)
		return nil, fmt.Errorf("failed to parse DataMarket ABI: %w", err)
	}

	// Create contract binding using ContractBackend (handles failover automatically)
	contract := bind.NewBoundContract(dataMarketAddress, parsedABI, c.contractBackend, c.contractBackend, nil)

	// Call dayCounter() view function
	var result []interface{}
	callOpts := c.GetCallOpts(ctx)
	err = contract.Call(callOpts, &result, "dayCounter")
	if err != nil {
		log.Printf("❌ FetchCurrentDay: Failed to call dayCounter on DataMarket contract %s: %v", dataMarketAddress.Hex(), err)
		return nil, fmt.Errorf("failed to call dayCounter on DataMarket contract %s: %w", dataMarketAddress.Hex(), err)
	}

	if len(result) == 0 {
		log.Printf("❌ FetchCurrentDay: dayCounter returned no result")
		return nil, fmt.Errorf("dayCounter returned no result")
	}

	day, ok := result[0].(*big.Int)
	if !ok {
		log.Printf("❌ FetchCurrentDay: dayCounter returned unexpected type: %T", result[0])
		return nil, fmt.Errorf("dayCounter returned unexpected type: %T", result[0])
	}

	log.Printf("✅ FetchCurrentDay: Successfully fetched day %s from DataMarket contract %s", day.String(), dataMarketAddress.Hex())
	return day, nil
}

// FetchDailySnapshotQuota fetches the daily snapshot quota for a data market from the DataMarket contract
func (c *Client) FetchDailySnapshotQuota(ctx context.Context, dataMarketAddress common.Address) (*big.Int, error) {
	if c.contractBackend == nil {
		return nil, fmt.Errorf("RPC helper not initialized - POWERLOOM_RPC_NODES or POWERLOOM_RPC_URL is required")
	}

	// Parse ABI
	parsedABI, err := parseABI(c.dataMarketABI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DataMarket ABI: %w", err)
	}

	// Create contract binding
	contract := bind.NewBoundContract(dataMarketAddress, parsedABI, c.contractBackend, c.contractBackend, nil)

	// Call dailySnapshotQuota() view function
	var result []interface{}
	callOpts := c.GetCallOpts(ctx)
	err = contract.Call(callOpts, &result, "dailySnapshotQuota")
	if err != nil {
		return nil, fmt.Errorf("failed to call dailySnapshotQuota on DataMarket contract %s: %w", dataMarketAddress.Hex(), err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("dailySnapshotQuota returned no result")
	}

	quota, ok := result[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("dailySnapshotQuota returned unexpected type: %T", result[0])
	}

	return quota, nil
}

// parseABI parses the ABI JSON string into an ABI object
func parseABI(abiJSON string) (abi.ABI, error) {
	parsedABI, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return abi.ABI{}, fmt.Errorf("failed to parse ABI JSON: %w", err)
	}
	return parsedABI, nil
}

// ShouldUpdate checks if we should update for this epoch
func (c *Client) ShouldUpdate(epochID uint64) bool {
	if c.updateEpochInterval <= 0 {
		return false
	}
	return epochID%uint64(c.updateEpochInterval) == 0
}

// GetUpdateMethod returns the update method being used
func (c *Client) GetUpdateMethod() string {
	return c.updateMethod
}

// Close closes RPC helper connections
func (c *Client) Close() {
	if c.rpcHelper != nil {
		// RPC helper manages its own connections, no explicit close needed
		// but we can clean up if needed
	}
}

// GetCallOpts returns call options for contract calls
// Note: The context passed in should already have appropriate timeout/cancellation
// This function does not add additional timeout to avoid double-wrapping contexts
func (c *Client) GetCallOpts(ctx context.Context) *bind.CallOpts {
	return &bind.CallOpts{
		Context: ctx,
	}
}
