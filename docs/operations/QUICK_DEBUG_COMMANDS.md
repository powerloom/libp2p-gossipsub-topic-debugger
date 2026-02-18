# Quick Debug Commands Reference

## Most Important Commands

### 1. Monitor All Critical Logs (Single Command)
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "FetchCurrentDay|Day changed|Day unchanged|CheckDayTransition|Day transition detected|No day transition|Stored day transition marker|IsBufferEpoch|Buffer epoch reached|UpdateFinalRewards"
```

### 2. Check if Day is Changing
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | \
  grep -E "Day changed|Day unchanged" | tail -20
```

### 3. Check if Transitions Are Detected
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | \
  grep -E "Day transition detected|No day transition" | tail -20
```

### 4. Check Redis Markers
```bash
# List all markers
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SMEMBERS "DayRolloverEpochMarkerSet.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"

# Get marker details (replace EPOCH with actual epoch from SMEMBERS)
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  GET "DayRolloverEpochMarkerDetails.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.EPOCH"
```

### 5. Check Buffer Epoch Matching
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f -n 1000 | \
  grep "IsBufferEpoch" | tail -20
```

### 6. Check if UpdateFinalRewards is Called (Comprehensive)
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "Buffer epoch reached|UpdateFinalRewards: ENTRY|UpdateFinalRewards:.*exiting early|Step 1 complete|Step 2|Successfully sent.*final rewards|Successfully sent final rewards update"
```

## Key Log Patterns to Look For

### ✅ Good Signs
- `✅ FetchCurrentDay: Successfully fetched day X`
- `📅 Day changed for data market ...: X -> Y`
- `📅 Day transition detected for data market ...: X -> Y`
- `📌 Stored day transition marker: ... bufferEpoch=XXXXX`
- `✅ IsBufferEpoch: Match found! Epoch X is buffer epoch`
- `🎯 Buffer epoch reached`
- `🎯 UpdateFinalRewards: ENTRY`

### ❌ Bad Signs
- `❌ FetchCurrentDay: Failed to call dayCounter`
- `📅 Day unchanged` (consistently)
- `📅 No day transition` (when day should have changed)
- `🔍 IsBufferEpoch: Found 0 marker(s)`
- `❌ IsBufferEpoch: No matching buffer epoch found`
- No `UpdateFinalRewards: ENTRY` logs at all

## Quick Diagnostic Questions

1. **Is day being fetched?** → Look for `FetchCurrentDay` logs
2. **Is day changing?** → Look for `Day changed` vs `Day unchanged`
3. **Are transitions detected?** → Look for `Day transition detected`
4. **Do markers exist?** → Run Redis `SMEMBERS` command
5. **Are buffer epochs matched?** → Look for `IsBufferEpoch` logs with matches
6. **Is UpdateFinalRewards called?** → Look for `UpdateFinalRewards: ENTRY`

## Expected Marker Format

When you GET a marker from Redis, you should see:
```json
{
  "last_known_day": "65",
  "current_epoch": 24285200,
  "buffer_epoch": 24285205
}
```

Verify: `buffer_epoch = current_epoch + BUFFER_EPOCHS` (default +5)
