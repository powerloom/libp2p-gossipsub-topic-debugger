package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// EpochWindowState tracks the state of an epoch window
type EpochWindowState string

const (
	WindowStateWaitingLevel1     EpochWindowState = "waiting_level1"     // Waiting for Level 1 finalization delay
	WindowStateCollectingBatches EpochWindowState = "collecting_batches" // Collecting validator batches
	WindowStateFinalized         EpochWindowState = "finalized"          // Window closed, tally finalized
)

// EpochWindow tracks window state for a specific epoch
type EpochWindow struct {
	EpochID              uint64
	DataMarketAddress    string
	ProtocolStateAddress string
	State                EpochWindowState
	EpochReleasedAt      time.Time
	Level1DelayEndsAt    time.Time
	AggregationEndsAt    time.Time
	FirstBatchArrivedAt  time.Time
	mu                   sync.RWMutex
}

// WindowManager manages epoch windows and timing
type WindowManager struct {
	ctx                       context.Context
	windows                   map[string]*EpochWindow // key: "epochID:dataMarket"
	mu                        sync.RWMutex
	level1FinalizationDelay   time.Duration
	aggregationWindowDuration time.Duration
	onWindowClose             func(epochID uint64, dataMarket string) error
}

// NewWindowManager creates a new window manager
func NewWindowManager(ctx context.Context) *WindowManager {
	level1Delay := 10 * time.Second // Default
	if delayStr := os.Getenv("LEVEL1_FINALIZATION_DELAY_SECONDS"); delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil {
			level1Delay = time.Duration(delay) * time.Second
		}
	}

	aggWindow := 20 * time.Second // Default
	if windowStr := os.Getenv("AGGREGATION_WINDOW_SECONDS"); windowStr != "" {
		if window, err := strconv.Atoi(windowStr); err == nil {
			aggWindow = time.Duration(window) * time.Second
		}
	}

	return &WindowManager{
		ctx:                       ctx,
		windows:                   make(map[string]*EpochWindow),
		level1FinalizationDelay:   level1Delay,
		aggregationWindowDuration: aggWindow,
	}
}

// SetWindowCloseCallback sets the callback to be called when a window closes
func (wm *WindowManager) SetWindowCloseCallback(callback func(epochID uint64, dataMarket string) error) {
	wm.onWindowClose = callback
}

// OnEpochReleased handles an EpochReleased event
func (wm *WindowManager) OnEpochReleased(event *EpochReleasedEvent) error {
	key := wm.getWindowKey(event.EpochID.Uint64(), event.DataMarketAddress.Hex())

	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Check if window already exists
	if _, exists := wm.windows[key]; exists {
		log.Printf("Window already exists for epoch %d, data market %s", event.EpochID.Uint64(), event.DataMarketAddress.Hex())
		return nil
	}

	now := time.Now()
	level1DelayEndsAt := now.Add(wm.level1FinalizationDelay)
	// Aggregation window starts after Level 1 delay completes
	aggregationEndsAt := level1DelayEndsAt.Add(wm.aggregationWindowDuration)

	window := &EpochWindow{
		EpochID:              event.EpochID.Uint64(),
		DataMarketAddress:    event.DataMarketAddress.Hex(),
		ProtocolStateAddress: event.DataMarketAddress.Hex(), // Will be set from config
		State:                WindowStateWaitingLevel1,
		EpochReleasedAt:      now,
		Level1DelayEndsAt:    level1DelayEndsAt,
		AggregationEndsAt:    aggregationEndsAt,
	}

	wm.windows[key] = window

	log.Printf("📅 Started epoch window: epoch=%d, dataMarket=%s, level1Delay=%v, aggWindow=%v",
		event.EpochID.Uint64(), event.DataMarketAddress.Hex(),
		wm.level1FinalizationDelay, wm.aggregationWindowDuration)

	// Start Level 1 delay timer
	go wm.startLevel1DelayTimer(window)

	return nil
}

// startLevel1DelayTimer waits for Level 1 finalization delay, then starts aggregation window
func (wm *WindowManager) startLevel1DelayTimer(window *EpochWindow) {
	delay := time.Until(window.Level1DelayEndsAt)
	if delay > 0 {
		log.Printf("⏳ Waiting %v for Level 1 finalization delay (epoch %d, dataMarket %s)",
			delay, window.EpochID, window.DataMarketAddress)
		time.Sleep(delay)
	}

	wm.mu.Lock()
	window.State = WindowStateCollectingBatches
	wm.mu.Unlock()

	log.Printf("✅ Level 1 delay completed, now accepting validator batches (epoch %d, dataMarket %s)",
		window.EpochID, window.DataMarketAddress)

	// Start aggregation window timer
	wm.startAggregationWindowTimer(window)
}

// startAggregationWindowTimer starts the aggregation window timer
func (wm *WindowManager) startAggregationWindowTimer(window *EpochWindow) {
	delay := time.Until(window.AggregationEndsAt)
	if delay > 0 {
		log.Printf("⏱️ Starting aggregation window: %v (epoch %d, dataMarket %s)",
			delay, window.EpochID, window.DataMarketAddress)
		time.Sleep(delay)
	}

	// Window expired - finalize
	wm.mu.Lock()
	window.State = WindowStateFinalized
	wm.mu.Unlock()

	log.Printf("⏰ Aggregation window expired - finalizing tally (epoch %d, dataMarket %s)",
		window.EpochID, window.DataMarketAddress)

	// Call callback
	if wm.onWindowClose != nil {
		if err := wm.onWindowClose(window.EpochID, window.DataMarketAddress); err != nil {
			log.Printf("Error in window close callback: %v", err)
		}
	}
}

// OnFirstBatchArrived handles the first batch arrival for an epoch
func (wm *WindowManager) OnFirstBatchArrived(epochID uint64, dataMarket string) error {
	key := wm.getWindowKey(epochID, dataMarket)

	wm.mu.RLock()
	window, exists := wm.windows[key]
	wm.mu.RUnlock()

	if !exists {
		log.Printf("Warning: Received batch for epoch %d, dataMarket %s but no window exists (epoch may have started before monitoring began)",
			epochID, dataMarket)
		return nil
	}

	window.mu.Lock()
	if window.FirstBatchArrivedAt.IsZero() {
		window.FirstBatchArrivedAt = time.Now()
		log.Printf("📦 First batch arrived for epoch %d, dataMarket %s (window state: %s)",
			epochID, dataMarket, window.State)
	}
	window.mu.Unlock()

	return nil
}

// CanAcceptBatch checks if batches can be accepted for an epoch
func (wm *WindowManager) CanAcceptBatch(epochID uint64, dataMarket string) bool {
	key := wm.getWindowKey(epochID, dataMarket)

	wm.mu.RLock()
	window, exists := wm.windows[key]
	wm.mu.RUnlock()

	if !exists {
		// No window exists - might be an old epoch or monitoring started late
		// Allow accepting batches anyway (will be handled gracefully)
		return true
	}

	window.mu.RLock()
	defer window.mu.RUnlock()

	// Can accept batches only if we're past Level 1 delay
	return window.State == WindowStateCollectingBatches || window.State == WindowStateFinalized
}

// IsWindowClosed checks if the aggregation window has closed for an epoch
func (wm *WindowManager) IsWindowClosed(epochID uint64, dataMarket string) bool {
	key := wm.getWindowKey(epochID, dataMarket)

	wm.mu.RLock()
	window, exists := wm.windows[key]
	wm.mu.RUnlock()

	if !exists {
		return false
	}

	window.mu.RLock()
	defer window.mu.RUnlock()

	return window.State == WindowStateFinalized
}

// GetWindowState returns the current window state
func (wm *WindowManager) GetWindowState(epochID uint64, dataMarket string) (EpochWindowState, error) {
	key := wm.getWindowKey(epochID, dataMarket)

	wm.mu.RLock()
	window, exists := wm.windows[key]
	wm.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("window not found for epoch %d, dataMarket %s", epochID, dataMarket)
	}

	window.mu.RLock()
	defer window.mu.RUnlock()

	return window.State, nil
}

// getWindowKey generates a key for the window map
func (wm *WindowManager) getWindowKey(epochID uint64, dataMarket string) string {
	return fmt.Sprintf("%d:%s", epochID, dataMarket)
}

// Cleanup removes old windows (older than 24 hours)
func (wm *WindowManager) Cleanup() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour)
	for key, window := range wm.windows {
		if window.EpochReleasedAt.Before(cutoff) {
			delete(wm.windows, key)
			log.Printf("Cleaned up old window: epoch %d, dataMarket %s", window.EpochID, window.DataMarketAddress)
		}
	}
}
