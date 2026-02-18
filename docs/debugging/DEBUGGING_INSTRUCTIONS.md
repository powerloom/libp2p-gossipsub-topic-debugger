# Debugging Instructions: Day Transition and Final Rewards Update

## Overview
This guide provides specific commands and log patterns to diagnose why `UpdateFinalRewards` is not being called.

## Key Log Patterns to Monitor

### 1. Day Fetching Logs
**What to look for**: Whether day is being fetched successfully and if it's changing

```bash
# Monitor all day fetching activity
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "FetchCurrentDay|Day changed|Day unchanged|First day fetch"

# Expected output when working correctly:
# ✅ FetchCurrentDay: Successfully fetched day 65 from DataMarket contract...
# 📅 Day unchanged for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: day 65 (epoch 24285171)
# 📅 Day changed for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: 65 -> 66 (epoch 24285200)

# Red flags:
# ❌ FetchCurrentDay: Failed to call dayCounter... (RPC failures)
# ⚠️ Could not fetch current day for epoch... (fetch failures)
```

**Investigation points**:
- If you see "Day unchanged" consistently, the contract's `dayCounter()` may not be updating
- If you see fetch failures, check RPC connectivity
- If day changes but transitions aren't detected, check transition detection logs

---

### 2. Day Transition Detection Logs
**What to look for**: Whether day transitions are being detected and markers created

```bash
# Monitor day transition detection
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "CheckDayTransition|Day transition detected|No day transition|Stored day transition marker"

# Expected output when transition detected:
# 📅 CheckDayTransition: dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, lastKnownDay=65, currentDay=66, epoch=24285200
# 📅 Day transition detected for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: 65 -> 66 (epoch 24285200)
# 📌 Stored day transition marker: dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, lastDay=65, bufferEpoch=24285205

# Expected output when NO transition:
# 📅 CheckDayTransition: dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, lastKnownDay=65, currentDay=65, epoch=24285171
# 📅 No day transition: dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, day unchanged at 65 (epoch 24285171)
```

**Investigation points**:
- If you see "No day transition" even when day changes, check if `lastKnownDay` is being updated correctly
- If transitions are detected but no markers created, check Redis connection
- Note the `bufferEpoch` value - it should be `currentEpoch + BUFFER_EPOCHS` (default +5)

---

### 3. Buffer Epoch Detection Logs
**What to look for**: Whether buffer epochs are being checked and matched

```bash
# Monitor buffer epoch checks
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "IsBufferEpoch|Buffer epoch reached|No matching buffer epoch"

# Expected output when buffer epoch matches:
# 🔍 IsBufferEpoch: Checking if epoch 24285205 is a buffer epoch for dataMarket 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371
# 🔍 IsBufferEpoch: Found 1 marker(s) in Redis for dataMarket 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371
# 🔍 IsBufferEpoch: Checking marker - epoch=24285200, lastDay=65, currentEpoch=24285200, bufferEpoch=24285205 (currentEpoch=24285205)
# ✅ IsBufferEpoch: Match found! Epoch 24285205 is buffer epoch for day transition (lastDay=65, transitionEpoch=24285200)
# 🎯 Buffer epoch reached for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: epoch 24285205 (previous day: 65)

# Expected output when NO match:
# 🔍 IsBufferEpoch: Checking if epoch 24285171 is a buffer epoch for dataMarket 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371
# 🔍 IsBufferEpoch: Found 0 marker(s) in Redis for dataMarket 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371
# ❌ IsBufferEpoch: No matching buffer epoch found for epoch 24285171, dataMarket 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371
```

**Investigation points**:
- If "Found 0 marker(s)" appears, markers weren't created or were deleted
- If markers exist but don't match, check the `bufferEpoch` calculation
- If Redis check fails, check Redis connectivity

---

### 4. UpdateFinalRewards Entry Logs
**What to look for**: Whether UpdateFinalRewards is being called at all

```bash
# Monitor UpdateFinalRewards calls
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "UpdateFinalRewards: ENTRY|UpdateFinalRewards:.*exiting early|Successfully sent final rewards update"

# Expected output when called:
# 🎯 UpdateFinalRewards: ENTRY - epoch=24285205, dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, day=65, eligibleNodes=1000, slotCount=1000
# ✅ Step 1 complete: Updated eligible nodes count (1000) for day 65
# ✅ Successfully sent all 2 batches for step 2 (final rewards update) for epoch 24285205, data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, day 65

# Expected output when exiting early:
# 🎯 UpdateFinalRewards: ENTRY - epoch=24285205, dataMarket=0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, day=65, eligibleNodes=0, slotCount=0
# ⚠️ UpdateFinalRewards: No submissions to update for final rewards: data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371, day 65 - exiting early

# If you see NO "UpdateFinalRewards: ENTRY" logs:
# → The function is never being called, which means IsBufferEpoch is returning false
```

**Investigation points**:
- If no "ENTRY" logs appear, `IsBufferEpoch` is returning false - check buffer epoch logs
- If "exiting early" appears, check why (disabled updates, no submissions, etc.)
- Note the `slotCount` and `eligibleNodes` values - should match expected counts

---

### 5. Comprehensive Monitoring Command
**Monitor all critical flows simultaneously**:

```bash
# Single command to monitor all key flows
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "FetchCurrentDay|Day changed|Day unchanged|CheckDayTransition|Day transition detected|No day transition|Stored day transition marker|IsBufferEpoch|Buffer epoch reached|UpdateFinalRewards"
```

---

## Redis Marker Investigation

### Check Redis Marker Set
**What to look for**: Whether day transition markers exist in Redis

```bash
# Connect to Redis container
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli

# List all marker sets (replace with your data market address, lowercase)
SMEMBERS "DayRolloverEpochMarkerSet.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"

# Expected output:
# 1) "24285200"  (epoch when transition was detected)

# If empty:
# (empty array)
# → No markers exist, transitions weren't detected or markers were deleted
```

### Check Marker Details
**What to look for**: Marker contents (lastDay, currentEpoch, bufferEpoch)

```bash
# Get marker details for a specific epoch (from SMEMBERS output above)
GET "DayRolloverEpochMarkerDetails.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.24285200"

# Expected output (JSON):
# {"last_known_day":"65","current_epoch":24285200,"buffer_epoch":24285205}

# Verify buffer_epoch calculation:
# buffer_epoch should equal current_epoch + BUFFER_EPOCHS (default 5)
# Example: 24285200 + 5 = 24285205 ✓
```

### Check Last Known Day
**What to look for**: What day the system thinks is current

```bash
# Get last known day
GET "LastKnownDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"

# Expected output:
# "66"  (or whatever the current day is)

# Compare with contract's actual dayCounter() value
```

---

## Step-by-Step Investigation Workflow

### Step 1: Verify Day Fetching
```bash
# Run for 10-20 epochs and check if day is changing
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | grep -E "FetchCurrentDay|Day changed|Day unchanged" | tail -20
```

**Questions to answer**:
- Is day being fetched successfully? (look for ✅ or ❌)
- Is day changing? (look for "Day changed" vs "Day unchanged")
- If day isn't changing, is the contract's dayCounter() actually updating?

---

### Step 2: Verify Day Transition Detection
```bash
# Check if transitions are being detected
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | grep -E "CheckDayTransition|Day transition detected|No day transition" | tail -20
```

**Questions to answer**:
- Are transitions being detected? (look for "Day transition detected")
- If day changes but no transition detected, check `lastKnownDay` vs `currentDay` in logs
- Are markers being created? (look for "Stored day transition marker")

---

### Step 3: Verify Marker Storage in Redis
```bash
# Check Redis for markers
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SMEMBERS "DayRolloverEpochMarkerSet.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
```

**Questions to answer**:
- Do markers exist? (should see epoch IDs)
- If markers exist, check their details (GET command above)
- Verify `buffer_epoch` calculation is correct

---

### Step 4: Verify Buffer Epoch Matching
```bash
# Check if buffer epochs are being matched
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | grep -E "IsBufferEpoch|Buffer epoch reached|No matching buffer epoch" | tail -20
```

**Questions to answer**:
- Are buffer epochs being checked every epoch? (should see "IsBufferEpoch" logs)
- Do markers exist when checking? (look for "Found X marker(s)")
- Why don't markers match? (check bufferEpoch vs currentEpoch in logs)

**Example analysis**:
```
🔍 IsBufferEpoch: Checking marker - epoch=24285200, lastDay=65, currentEpoch=24285200, bufferEpoch=24285205 (currentEpoch=24285171)
🔍 IsBufferEpoch: Marker doesn't match - bufferEpoch=24285205 != currentEpoch=24285171
```
→ Current epoch (24285171) hasn't reached buffer epoch (24285205) yet - this is normal, wait for buffer epoch

---

### Step 5: Verify UpdateFinalRewards Calls
```bash
# Check if UpdateFinalRewards is being called
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 10000 | grep "UpdateFinalRewards: ENTRY"
```

**Questions to answer**:
- Is UpdateFinalRewards being called? (should see "ENTRY" logs)
- If not called, go back to Step 4 - IsBufferEpoch is returning false
- If called but exiting early, check why (disabled updates, no submissions, etc.)

---

## Common Issues and Solutions

### Issue 1: Day Not Changing
**Symptoms**: Consistent "Day unchanged" logs, day stays at 65

**Investigation**:
```bash
# Check contract's actual dayCounter value (if you have access to RPC)
# Or check if day transitions are happening in contract events
```

**Possible causes**:
- Contract's dayCounter() isn't updating (contract issue)
- RPC is returning cached/stale data
- Day size configuration mismatch

---

### Issue 2: Day Changes But No Transition Detected
**Symptoms**: "Day changed" logs but no "Day transition detected"

**Investigation**:
```bash
# Check CheckDayTransition logs for comparison details
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep "CheckDayTransition"
```

**Possible causes**:
- `lastKnownDay` not being persisted correctly
- Day transition manager state lost on restart
- Race condition between day fetch and transition check

---

### Issue 3: Markers Created But Not Matched
**Symptoms**: Markers exist in Redis but "No matching buffer epoch found"

**Investigation**:
```bash
# Check marker details and compare with current epoch
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  GET "DayRolloverEpochMarkerDetails.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.<epoch>"

# Check IsBufferEpoch logs for mismatch details
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep "IsBufferEpoch"
```

**Possible causes**:
- Buffer epoch calculation wrong (check BUFFER_EPOCHS env var)
- Epoch numbers don't match (check epoch source)
- Markers deleted before buffer epoch reached

---

### Issue 4: UpdateFinalRewards Never Called
**Symptoms**: No "UpdateFinalRewards: ENTRY" logs

**Investigation**:
```bash
# Check if buffer epochs are being reached
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "IsBufferEpoch|Buffer epoch reached"
```

**Possible causes**:
- Buffer epochs never reached (markers deleted, epoch skipped)
- IsBufferEpoch returning false (check logs)
- Day transitions never detected (go back to Step 1)

---

## Quick Diagnostic Script

Save this as `check_day_transitions.sh`:

```bash
#!/bin/bash

DATA_MARKET="0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
CONTAINER="libp2p-gossipsub-topic-debugger-p2p-debugger-1"
REDIS_CONTAINER="libp2p-gossipsub-topic-debugger-redis-1"

echo "=== Day Fetching Status ==="
sudo docker logs $CONTAINER -n 1000 | grep -E "FetchCurrentDay|Day changed|Day unchanged" | tail -5

echo ""
echo "=== Day Transition Detection ==="
sudo docker logs $CONTAINER -n 1000 | grep -E "CheckDayTransition|Day transition detected|No day transition" | tail -5

echo ""
echo "=== Redis Markers ==="
sudo docker exec $REDIS_CONTAINER redis-cli SMEMBERS "DayRolloverEpochMarkerSet.$DATA_MARKET"

echo ""
echo "=== Buffer Epoch Checks ==="
sudo docker logs $CONTAINER -n 1000 | grep "IsBufferEpoch" | tail -5

echo ""
echo "=== UpdateFinalRewards Calls ==="
sudo docker logs $CONTAINER -n 10000 | grep "UpdateFinalRewards: ENTRY" | tail -5
```

Make it executable and run:
```bash
chmod +x check_day_transitions.sh
./check_day_transitions.sh
```

---

## Expected Flow Summary

**Normal flow when working correctly**:

1. **Every epoch**: Day fetched → "Day unchanged" or "Day changed"
2. **When day changes**: Transition detected → Marker created → Buffer epoch calculated
3. **Every epoch**: IsBufferEpoch checked → "No matching buffer epoch" (until buffer epoch reached)
4. **At buffer epoch**: IsBufferEpoch matches → "Buffer epoch reached" → UpdateFinalRewards called
5. **After final rewards**: Marker removed → Counts reset for previous day

**Current broken flow**:

1. Day fetched → Day stays at 65 (or changes but not detected)
2. No transitions detected → No markers created
3. IsBufferEpoch checked → "Found 0 marker(s)" → No match
4. UpdateFinalRewards never called

Use the logs above to identify where the flow breaks.
