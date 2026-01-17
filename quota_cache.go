package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"

	"p2p-debugger/contract"
	"p2p-debugger/redis"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// QuotaUpdateEpochInterval is how often to query quota from contract (every N epochs)
	QuotaUpdateEpochInterval = 1 // Query every epoch to catch quota changes
	// QuotaCacheEpochs is how many epochs of quota history to keep
	QuotaCacheEpochs = 10
)

// QuotaCache manages dailySnapshotQuota caching and periodic updates
type QuotaCache struct {
	ctx            context.Context
	contractClient *contract.Client
	quotaCache     map[string]*big.Int             // dataMarket -> quota (in-memory cache)
	quotaHistory   map[string][]*QuotaHistoryEntry // dataMarket -> []{epoch, quota}
	mu             sync.RWMutex
	updateInterval uint64
}

// QuotaHistoryEntry stores quota value for a specific epoch
type QuotaHistoryEntry struct {
	Epoch uint64
	Quota *big.Int
}

// NewQuotaCache creates a new quota cache manager
func NewQuotaCache(ctx context.Context, contractClient *contract.Client) *QuotaCache {
	return &QuotaCache{
		ctx:            ctx,
		contractClient: contractClient,
		quotaCache:     make(map[string]*big.Int),
		quotaHistory:   make(map[string][]*QuotaHistoryEntry),
		updateInterval: QuotaUpdateEpochInterval,
	}
}

// GetQuota returns the cached quota for a data market, or fetches from Redis/contract if not cached
func (qc *QuotaCache) GetQuota(dataMarket string) (*big.Int, error) {
	return qc.GetQuotaWithContext(qc.ctx, dataMarket)
}

// GetQuotaWithContext returns the cached quota for a data market with a custom context
func (qc *QuotaCache) GetQuotaWithContext(ctx context.Context, dataMarket string) (*big.Int, error) {
	// Try in-memory cache first
	qc.mu.RLock()
	if quota, ok := qc.quotaCache[dataMarket]; ok {
		qc.mu.RUnlock()
		return quota, nil
	}
	qc.mu.RUnlock()

	// Try Redis cache
	if redis.RedisClient != nil {
		quotaStr, err := redis.HGet(ctx, redis.DailySnapshotQuotaTableKey(), dataMarket)
		if err == nil && quotaStr != "" {
			quota, ok := new(big.Int).SetString(quotaStr, 10)
			if ok {
				// Update in-memory cache
				qc.mu.Lock()
				qc.quotaCache[dataMarket] = quota
				qc.mu.Unlock()
				return quota, nil
			}
		}
	}

	// Fallback to contract query
	if qc.contractClient != nil {
		quota, err := qc.contractClient.FetchDailySnapshotQuota(ctx, common.HexToAddress(dataMarket))
		if err == nil {
			// Cache in memory and Redis
			qc.updateCache(dataMarket, quota)
			return quota, nil
		}
		return nil, fmt.Errorf("failed to fetch quota from contract: %w", err)
	}

	return nil, fmt.Errorf("no quota available and contract client not initialized")
}

// UpdateQuotaForEpoch updates quota cache for a specific epoch
// Should be called periodically (every epoch or every N epochs)
func (qc *QuotaCache) UpdateQuotaForEpoch(dataMarket string, epochID uint64) error {
	return qc.UpdateQuotaForEpochWithContext(qc.ctx, dataMarket, epochID)
}

// UpdateQuotaForEpochWithContext updates quota cache for a specific epoch with a custom context
func (qc *QuotaCache) UpdateQuotaForEpochWithContext(ctx context.Context, dataMarket string, epochID uint64) error {
	if qc.contractClient == nil {
		return fmt.Errorf("contract client not initialized")
	}

	// Check if we should update (based on interval)
	if epochID%qc.updateInterval != 0 {
		return nil // Skip this epoch
	}

	// Fetch quota from contract
	quota, err := qc.contractClient.FetchDailySnapshotQuota(ctx, common.HexToAddress(dataMarket))
	if err != nil {
		return fmt.Errorf("failed to fetch quota for epoch %d: %w", epochID, err)
	}

	// Update cache
	qc.updateCache(dataMarket, quota)

	// Add to history (keep last N epochs)
	qc.mu.Lock()
	history := qc.quotaHistory[dataMarket]
	if history == nil {
		history = make([]*QuotaHistoryEntry, 0)
	}

	// Add new entry
	history = append(history, &QuotaHistoryEntry{
		Epoch: epochID,
		Quota: new(big.Int).Set(quota),
	})

	// Keep only last QuotaCacheEpochs entries
	if len(history) > QuotaCacheEpochs {
		history = history[len(history)-QuotaCacheEpochs:]
	}

	qc.quotaHistory[dataMarket] = history
	qc.mu.Unlock()

	log.Printf("📊 Updated quota cache for data market %s, epoch %d: %s", dataMarket, epochID, quota.String())
	return nil
}

// updateCache updates both in-memory and Redis cache
func (qc *QuotaCache) updateCache(dataMarket string, quota *big.Int) {
	// Update in-memory cache
	qc.mu.Lock()
	qc.quotaCache[dataMarket] = new(big.Int).Set(quota)
	qc.mu.Unlock()

	// Update Redis cache
	if redis.RedisClient != nil {
		quotaStr := quota.String()
		if err := redis.HSet(qc.ctx, redis.DailySnapshotQuotaTableKey(), dataMarket, quotaStr); err != nil {
			log.Printf("⚠️ Failed to update quota in Redis cache: %v", err)
		} else {
			log.Printf("✅ Updated quota in Redis cache for data market %s: %s", dataMarket, quotaStr)
		}
	}
}

// GetQuotaHistory returns quota history for a data market (last N epochs)
func (qc *QuotaCache) GetQuotaHistory(dataMarket string) []*QuotaHistoryEntry {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	history := qc.quotaHistory[dataMarket]
	if history == nil {
		return []*QuotaHistoryEntry{}
	}

	// Return a copy
	result := make([]*QuotaHistoryEntry, len(history))
	for i, entry := range history {
		result[i] = &QuotaHistoryEntry{
			Epoch: entry.Epoch,
			Quota: new(big.Int).Set(entry.Quota),
		}
	}
	return result
}

// LoadFromRedis loads quota cache from Redis on startup
func (qc *QuotaCache) LoadFromRedis(dataMarkets []string) {
	if redis.RedisClient == nil {
		return
	}

	for _, dataMarket := range dataMarkets {
		quotaStr, err := redis.HGet(qc.ctx, redis.DailySnapshotQuotaTableKey(), dataMarket)
		if err == nil && quotaStr != "" {
			quota, ok := new(big.Int).SetString(quotaStr, 10)
			if ok {
				qc.mu.Lock()
				qc.quotaCache[dataMarket] = quota
				qc.mu.Unlock()
				log.Printf("✅ Loaded quota from Redis for data market %s: %s", dataMarket, quotaStr)
			}
		}
	}
}
