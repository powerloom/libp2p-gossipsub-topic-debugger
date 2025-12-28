package contract

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"
)

// UpdateRewardsRequest represents the request payload for updateRewards
type UpdateRewardsRequest struct {
	DataMarketAddress string     `json:"dataMarketAddress"`
	SlotIDs           []*big.Int `json:"slotIDs"`
	SubmissionsList   []*big.Int `json:"submissionsList"`
	Day               *big.Int   `json:"day"`
	EligibleNodes     int        `json:"eligibleNodes"`
	AuthToken         string     `json:"authToken"`
}

// RelayerClient handles HTTP requests to the relayer service
type RelayerClient struct {
	url    string
	client *http.Client
	authToken string
}

// NewRelayerClient creates a new relayer client
func NewRelayerClient(url string, authToken string) *RelayerClient {
	return &RelayerClient{
		url: url,
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

