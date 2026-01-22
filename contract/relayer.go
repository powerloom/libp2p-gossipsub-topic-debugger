package contract

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"
)

// UpdateRewardsRequest represents the request payload for updateRewards (deprecated, kept for backward compatibility)
type UpdateRewardsRequest struct {
	MessageType       string     `json:"messageType"`
	DataMarketAddress string     `json:"dataMarketAddress"`
	SlotIDs           []*big.Int `json:"slotIDs"`
	SubmissionsList   []*big.Int `json:"submissionsList"`
	Day               *big.Int   `json:"day"`
	EligibleNodes     int        `json:"eligibleNodes"`
	AuthToken         string     `json:"authToken"`
}

// UpdateSubmissionCountsRequest represents the request payload for periodic submission count updates
type UpdateSubmissionCountsRequest struct {
	MessageType       string     `json:"messageType"`
	DataMarketAddress string     `json:"dataMarketAddress"`
	SlotIDs           []*big.Int `json:"slotIDs"`
	SubmissionsList   []*big.Int `json:"submissionsList"`
	Day               *big.Int   `json:"day"`
	AuthToken         string     `json:"authToken"`
}

// UpdateEligibleNodesRequest represents the request payload for updating eligible nodes (Step 1)
type UpdateEligibleNodesRequest struct {
	MessageType       string   `json:"messageType"`
	DataMarketAddress string   `json:"dataMarketAddress"`
	Day               *big.Int `json:"day"`
	EligibleNodes     int      `json:"eligibleNodes"`
	AuthToken         string   `json:"authToken"`
}

// UpdateEligibleSubmissionCountsRequest represents the request payload for updating eligible submission counts (Step 2)
type UpdateEligibleSubmissionCountsRequest struct {
	MessageType       string     `json:"messageType"`
	DataMarketAddress string     `json:"dataMarketAddress"`
	SlotIDs           []*big.Int `json:"slotIDs"`
	SubmissionsList   []*big.Int `json:"submissionsList"`
	Day               *big.Int   `json:"day"`
	AuthToken         string     `json:"authToken"`
}

// RelayerClient handles HTTP requests to the relayer service
type RelayerClient struct {
	url       string
	client    *http.Client
	authToken string
}

// NewRelayerClient creates a new relayer client
func NewRelayerClient(url string, authToken string) *RelayerClient {
	return &RelayerClient{
		url:       url,
		authToken: authToken,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

// SendUpdateRewards sends an updateRewards request to the relayer
func (rc *RelayerClient) SendUpdateRewards(ctx context.Context, dataMarketAddress string, slotIDs, submissionsList []*big.Int, day *big.Int, eligibleNodes int) error {
	request := UpdateRewardsRequest{
		DataMarketAddress: dataMarketAddress,
		SlotIDs:           slotIDs,
		SubmissionsList:   submissionsList,
		Day:               day,
		EligibleNodes:     eligibleNodes,
		AuthToken:         rc.authToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal update rewards request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/submitUpdateRewards", rc.url), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rc.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update rewards request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send update rewards request, status code: %d", resp.StatusCode)
	}

	return nil
}

// SendUpdateSubmissionCounts sends a periodic submission count update request to the relayer
func (rc *RelayerClient) SendUpdateSubmissionCounts(ctx context.Context, dataMarketAddress string, slotIDs, submissionsList []*big.Int, day *big.Int) error {
	request := UpdateSubmissionCountsRequest{
		MessageType:       "UpdateSubmissionCounts",
		DataMarketAddress: dataMarketAddress,
		SlotIDs:           slotIDs,
		SubmissionsList:   submissionsList,
		Day:               day,
		AuthToken:         rc.authToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal update submission counts request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/submitUpdateSubmissionCounts", rc.url), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rc.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update submission counts request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send update submission counts request, status code: %d", resp.StatusCode)
	}

	return nil
}

// SendUpdateEligibleNodes sends an eligible nodes update request to the relayer (Step 1)
func (rc *RelayerClient) SendUpdateEligibleNodes(ctx context.Context, dataMarketAddress string, day *big.Int, eligibleNodes int) error {
	request := UpdateEligibleNodesRequest{
		MessageType:       "UpdateEligibleNodes",
		DataMarketAddress: dataMarketAddress,
		Day:               day,
		EligibleNodes:     eligibleNodes,
		AuthToken:         rc.authToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal update eligible nodes request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/submitUpdateEligibleNodes", rc.url), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := rc.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update eligible nodes request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send update eligible nodes request, status code: %d", resp.StatusCode)
	}

	return nil
}

// SendUpdateEligibleSubmissionCounts sends an eligible submission counts update request to the relayer (Step 2)
func (rc *RelayerClient) SendUpdateEligibleSubmissionCounts(ctx context.Context, dataMarketAddress string, slotIDs, submissionsList []*big.Int, day *big.Int) error {
	request := UpdateEligibleSubmissionCountsRequest{
		MessageType:       "UpdateEligibleSubmissionCounts",
		DataMarketAddress: dataMarketAddress,
		SlotIDs:           slotIDs,
		SubmissionsList:   submissionsList,
		Day:               day,
		AuthToken:         rc.authToken,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal update eligible submission counts request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/submitUpdateEligibleSubmissionCounts", rc.url), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	log.Printf("📤 [Step 2] Sending batch to relayer: day=%s, slots=%d, firstSlot=%s, lastSlot=%s",
		day.String(), len(slotIDs), slotIDs[0].String(), slotIDs[len(slotIDs)-1].String())

	resp, err := rc.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update eligible submission counts request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body to check for errors
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("❌ [Step 2] Relayer returned error: status=%d, body=%s", resp.StatusCode, string(body))
		return fmt.Errorf("failed to send update eligible submission counts request, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	log.Printf("✅ [Step 2] Relayer accepted batch: day=%s, slots=%d, response=%s", day.String(), len(slotIDs), string(body))
	return nil
}
