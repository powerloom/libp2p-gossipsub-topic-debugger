package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"p2p-debugger/redis"
)

const (
	// BufferEpochs is the number of epochs to wait after day transition before sending final update
	BufferEpochs = 5
)

// DayTransitionManager manages day transitions and buffer epoch tracking
type DayTransitionManager struct {
	ctx                  context.Context
	lastKnownDay         map[string]string             // dataMarket -> last known day
	dayTransitionMarkers map[string]*DayTransitionInfo // "dataMarket:epochID" -> DayTransitionInfo
	mu                   sync.RWMutex
	bufferEpochs         int64
}

// NewDayTransitionManager creates a new day transition manager
func NewDayTransitionManager(ctx context.Context) *DayTransitionManager {
	bufferEpochs := int64(BufferEpochs)
	if bufferStr := os.Getenv("BUFFER_EPOCHS"); bufferStr != "" {
		if buffer, err := strconv.ParseInt(bufferStr, 10, 64); err == nil {
			bufferEpochs = buffer
		}
	}

	return &DayTransitionManager{
		ctx:                  ctx,
		lastKnownDay:         make(map[string]string),
		dayTransitionMarkers: make(map[string]*DayTransitionInfo),
		bufferEpochs:         bufferEpochs,
	}
}

// CheckDayTransition checks if a day transition occurred and stores marker if needed
func (dtm *DayTransitionManager) CheckDayTransition(dataMarket string, currentDay string, currentEpoch uint64) bool {
	dtm.mu.Lock()
	defer dtm.mu.Unlock()

	lastDay, exists := dtm.lastKnownDay[dataMarket]

	// First time seeing this data market
	if !exists {
		dtm.lastKnownDay[dataMarket] = currentDay
		log.Printf("📅 Initialized day tracking for data market %s: day %s", dataMarket, currentDay)
		return false
	}

	// Day transition detected
	if lastDay != currentDay {
		log.Printf("📅 Day transition detected for data market %s: %s -> %s (epoch %d)",
			dataMarket, lastDay, currentDay, currentEpoch)

		// Create day transition marker
		bufferEpoch := int64(currentEpoch) + dtm.bufferEpochs
		marker := &DayTransitionInfo{
			LastKnownDay: lastDay,
			CurrentEpoch: int64(currentEpoch),
			BufferEpoch:  bufferEpoch,
		}

		// Store marker in-memory
		markerKey := fmt.Sprintf("%s:%d", dataMarket, currentEpoch)
		dtm.dayTransitionMarkers[markerKey] = marker
		dtm.lastKnownDay[dataMarket] = currentDay

		// Persist marker to Redis
		epochIDStr := strconv.FormatUint(currentEpoch, 10)
		if redis.RedisClient != nil {
			markerJSON, err := json.Marshal(marker)
			if err == nil {
				// Add to set of epoch markers
				if err := redis.SAdd(dtm.ctx, redis.DayRolloverEpochMarkerSet(dataMarket), epochIDStr); err != nil {
					log.Printf("⚠️ Failed to add epoch %s to day rollover marker set: %v", epochIDStr, err)
				}
				// Store marker details
				if err := redis.Set(dtm.ctx, redis.DayRolloverEpochMarkerDetails(dataMarket, epochIDStr), string(markerJSON)); err != nil {
					log.Printf("⚠️ Failed to store day rollover marker details: %v", err)
				} else {
					log.Printf("✅ Persisted day transition marker to Redis: dataMarket=%s, epoch=%s", dataMarket, epochIDStr)
				}
			} else {
				log.Printf("⚠️ Failed to marshal day transition marker: %v", err)
			}
		}

		// Also update last known day in Redis
		if redis.RedisClient != nil {
			if err := redis.Set(dtm.ctx, redis.LastKnownDayKey(dataMarket), currentDay); err != nil {
				log.Printf("⚠️ Failed to update last known day in Redis: %v", err)
			}
		}

		log.Printf("📌 Stored day transition marker: dataMarket=%s, lastDay=%s, bufferEpoch=%d",
			dataMarket, lastDay, bufferEpoch)
		return true
	}

	return false
}

// IsBufferEpoch checks if the current epoch is a buffer epoch for any day transition
// Checks Redis first (source of truth), falls back to in-memory cache
func (dtm *DayTransitionManager) IsBufferEpoch(dataMarket string, currentEpoch uint64) (*DayTransitionInfo, bool) {
	// Try Redis first (source of truth)
	if redis.RedisClient != nil {
		epochMarkerKeys, err := redis.SMembers(dtm.ctx, redis.DayRolloverEpochMarkerSet(dataMarket))
		if err == nil {
			for _, epochIDStr := range epochMarkerKeys {
				// Fetch marker details from Redis
				markerJSON, err := redis.Get(dtm.ctx, redis.DayRolloverEpochMarkerDetails(dataMarket, epochIDStr))
				if err == nil && markerJSON != "" {
					var marker DayTransitionInfo
					if err := json.Unmarshal([]byte(markerJSON), &marker); err == nil {
						if marker.BufferEpoch == int64(currentEpoch) {
							return &marker, true
						}
					}
				}
			}
		}
	}

	// Fallback to in-memory cache
	dtm.mu.RLock()
	defer dtm.mu.RUnlock()

	// Check all markers for this data market
	for key, marker := range dtm.dayTransitionMarkers {
		// Extract data market from key (format: "dataMarket:epochID")
		if len(key) > len(dataMarket) && key[:len(dataMarket)] == dataMarket {
			if marker.BufferEpoch == int64(currentEpoch) {
				return marker, true
			}
		}
	}

	return nil, false
}

// RemoveMarker removes a day transition marker after final update is sent
func (dtm *DayTransitionManager) RemoveMarker(dataMarket string, epochID int64) {
	dtm.mu.Lock()
	defer dtm.mu.Unlock()

	markerKey := fmt.Sprintf("%s:%d", dataMarket, epochID)
	delete(dtm.dayTransitionMarkers, markerKey)

	// Also remove from Redis
	epochIDStr := strconv.FormatInt(epochID, 10)
	if redis.RedisClient != nil {
		// Remove from set
		if err := redis.SRem(dtm.ctx, redis.DayRolloverEpochMarkerSet(dataMarket), epochIDStr); err != nil {
			log.Printf("⚠️ Failed to remove epoch %s from day rollover marker set: %v", epochIDStr, err)
		}
		// Set expiration on marker details for cleanup (30 minutes)
		if err := redis.Expire(dtm.ctx, redis.DayRolloverEpochMarkerDetails(dataMarket, epochIDStr), 30*time.Minute); err != nil {
			log.Printf("⚠️ Failed to set expiry for marker details: %v", err)
		}
	}

	log.Printf("🧹 Removed day transition marker for data market %s, epoch %d", dataMarket, epochID)
}

// GetLastKnownDay returns the last known day for a data market
func (dtm *DayTransitionManager) GetLastKnownDay(dataMarket string) string {
	dtm.mu.RLock()
	defer dtm.mu.RUnlock()
	return dtm.lastKnownDay[dataMarket]
}
