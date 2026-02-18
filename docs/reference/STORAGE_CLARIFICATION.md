# Storage Clarification: Eligible Nodes Are Stored Per Day

## Confirmation: Storage is Per Day ✅

**Yes, eligible nodes are definitely stored per day**, not per epoch. Here's the proof:

### Redis Key Structure

All Redis keys include the **day** as part of the key:

1. **Slot Submission Count** (accumulated per day):
   ```
   SlotSubmissions.{dataMarket}.{day}.{slotID}
   ```
   Example: `SlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234`

2. **Eligible Slot Submission Count** (accumulated per day):
   ```
   EligibleSlotSubmissions.{dataMarket}.{day}.{slotID}
   ```
   Example: `EligibleSlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234`

3. **Eligible Nodes Set** (per day):
   ```
   EligibleNodesByDay.{dataMarket}.{day}
   ```
   Example: `EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65`
   - This is a Redis SET containing all slot IDs that have submissions for that day

### How Counts Accumulate

When `UpdateEligibleCountsForDay` is called:

```go
// Line 222: Accumulate in memory per day
sc.counts[dataMarket][day][slotID] += count

// Line 230: Increment Redis counter per day
redis.IncrBy(key, count)  // key includes day

// Line 242: Update eligible count per day
redis.Set(eligibleKey, newCount)  // eligibleKey includes day

// Line 257: Add to eligible nodes set per day
redis.SAdd(eligibleNodesKey, slotIDStr)  // eligibleNodesKey includes day
```

**Key Point**: The `IncrBy` operation **accumulates** counts across epochs within the same day. Each epoch adds to the same day-specific key.

### Why Logs Mention Epoch

The log message at line 264 includes epoch for **context** only:

```go
log.Printf("Updated eligible counts for epoch %d, dataMarket %s, day %s: %d slots",
    epochID, dataMarket, day, len(slotCounts))
```

This log says:
- **"for epoch X"** = This update happened during epoch X (informational context)
- **"day Y"** = Counts are being stored/accumulated for day Y (the actual storage key)

**The epoch is just metadata** - the actual storage key is per day.

### Epoch-Specific Tracking (Separate from Main Storage)

There IS epoch-specific tracking (line 246-251), but it's **separate** and used for reporting/debugging:

```go
// Epoch-specific key (for tracking/reporting only)
epochKey := redis.EligibleSlotSubmissionsByEpochKey(dataMarket, day, epochIDStr)
// Format: EligibleSlotSubmissionsByEpoch.{dataMarket}.{day}.{epochID}
```

This stores counts **by epoch within a day**, but:
- It's NOT used for final rewards calculation
- It's NOT used for `GetCountsForDay` or `GetEligibleNodesCountForDay`
- It's just for tracking/debugging purposes

### How Final Rewards Uses Day-Based Storage

When `UpdateFinalRewards` is called:

```go
// Line 592: Get counts for the PREVIOUS DAY (not epoch)
prevDaySlotCounts := submissionCounter.GetCountsForDay(dataMarket, marker.LastKnownDay)

// Line 594: Get eligible nodes count for the PREVIOUS DAY
prevDayEligibleNodes := submissionCounter.GetEligibleNodesCountForDay(dataMarket, marker.LastKnownDay, dailySnapshotQuota)
```

Both functions read from **day-based keys**, not epoch-based keys.

### Verification Commands

To verify storage is per day:

```bash
# Check Redis keys for day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  KEYS "*EligibleNodesByDay*65*"

# Check slot submission count for a specific slot on day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  GET "SlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234"

# Check eligible nodes set for day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SMEMBERS "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"
```

### Summary

✅ **Storage is per day** - All Redis keys include `{day}`  
✅ **Counts accumulate per day** - Multiple epochs add to the same day key  
✅ **Epoch in logs is just context** - Not part of the storage key  
⚠️ **Epoch-specific keys exist** - But only for tracking/debugging, not used for final rewards  

The confusion comes from logs mentioning epoch, but the actual storage is definitely per day.
