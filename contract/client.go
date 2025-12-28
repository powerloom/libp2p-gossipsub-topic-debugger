package contract

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Client handles contract interactions
type Client struct {
	client              *ethclient.Client
	protocolContract    common.Address
	updateMethod        string // "direct" or "relayer"
	rpcURL              string
	relayerURL          string
	relayerAuthToken    string
	updateEpochInterval int64
}

// NewClient creates a new contract client
func NewClient() (*Client, error) {
	// Check if contract updates are enabled
	enabled := os.Getenv("ENABLE_CONTRACT_UPDATES") == "true"
	if !enabled {
		return &Client{
			updateMethod:        "disabled",
			updateEpochInterval: 0,
		}, nil
	}

	updateMethod := os.Getenv("CONTRACT_UPDATE_METHOD")
	if updateMethod == "" {
		updateMethod = "relayer" // Default to relayer
	}

	rpcURL := os.Getenv("POWERLOOM_RPC_URL")
	relayerURL := os.Getenv("RELAYER_URL")
	relayerAuthToken := os.Getenv("RELAYER_AUTH_TOKEN")

	protocolContractStr := os.Getenv("PROTOCOL_STATE_CONTRACT")
	if protocolContractStr == "" {
		return nil, fmt.Errorf("PROTOCOL_STATE_CONTRACT environment variable is required")
	}
	protocolContract := common.HexToAddress(protocolContractStr)

	updateInterval := int64(10) // Default 10 epochs
	if intervalStr := os.Getenv("SUBMISSION_UPDATE_EPOCH_INTERVAL"); intervalStr != "" {
		if interval, err := strconv.ParseInt(intervalStr, 10, 64); err == nil {
			updateInterval = interval
		}
	}

	client := &Client{
		protocolContract:    protocolContract,
		updateMethod:        updateMethod,
		rpcURL:              rpcURL,
		relayerURL:          relayerURL,
		relayerAuthToken:    relayerAuthToken,
		updateEpochInterval: updateInterval,
	}

	// Initialize ethclient if using direct method
	if updateMethod == "direct" {
		if rpcURL == "" {
			return nil, fmt.Errorf("POWERLOOM_RPC_URL is required when CONTRACT_UPDATE_METHOD=direct")
		}

		ethClient, err := ethclient.Dial(rpcURL)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to RPC: %w", err)
		}
		client.client = ethClient
	}

	return client, nil
}

// FetchCurrentDay fetches the current day for a data market from the protocol contract
func (c *Client) FetchCurrentDay(ctx context.Context, dataMarketAddress common.Address) (*big.Int, error) {
	if c.updateMethod == "direct" && c.client != nil {
		// Call contract method to get current day
		// This would require the contract ABI - for now, return error
		// In production, this would be: c.protocolContract.DayCounter(&bind.CallOpts{Context: ctx}, dataMarketAddress)
		return nil, fmt.Errorf("direct contract calls not fully implemented - use relayer method")
	}

	// For relayer method, we'd need to query via API or cache
	// For now, return error indicating this needs to be implemented
	return nil, fmt.Errorf("FetchCurrentDay not implemented for relayer method - needs API endpoint")
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

// Close closes the client connection
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// GetCallOpts returns call options for contract calls
func (c *Client) GetCallOpts(ctx context.Context) *bind.CallOpts {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel() // Ensure context is cancelled to avoid leak
	return &bind.CallOpts{
		Context: callCtx,
	}
}
