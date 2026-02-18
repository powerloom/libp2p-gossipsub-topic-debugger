# Quick Test Monitoring Guide

## Setup
- **Day Size**: 150 Ethereum mainnet blocks
- **Expected Day Transition**: ~30 minutes

## Real-Time Monitoring Commands

### 1. Monitor All Critical Flows (Single Terminal)
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "FetchCurrentDay|Day changed|Day unchanged|CheckDayTransition|Day transition detected|No day transition|Stored day transition marker|IsBufferEpoch|Buffer epoch reached|UpdateFinalRewards"
```

### 2. Monitor Day Changes Specifically
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "Day changed|Day unchanged|First day fetch"
```

### 3. Monitor Day Transition Detection
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E "CheckDayTransition|Day transition detected|No day transition|Stored day transition marker"
```

### 4. Monitor Buffer Epoch Matching
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep "IsBufferEpoch"
```

### 5. Monitor UpdateFinalRewards Calls (Comprehensive)
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "Buffer epoch reached|UpdateFinalRewards: ENTRY|UpdateFinalRewards:.*exiting early|Step 1 complete|Step 2|Successfully sent.*final rewards|Successfully sent final rewards update"
```

## What to Look For (Expected Sequence)

### Step 1: Day Transition Detected
```
📅 Day changed for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: X -> Y (epoch ZZZZ)
📅 Day transition detected for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: X -> Y (epoch ZZZZ)
📌 Stored day transition marker: dataMarket=..., lastDay=X, bufferEpoch=ZZZZ+5
```

### Step 2: Buffer Epoch Reached (5 epochs later)
```
🔍 IsBufferEpoch: Checking if epoch ZZZZ+5 is a buffer epoch...
✅ IsBufferEpoch: Match found! Epoch ZZZZ+5 is buffer epoch for day transition
🎯 Buffer epoch reached for data market 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371: epoch ZZZZ+5 (previous day: X)
```

### Step 3: UpdateFinalRewards Called
```
🎯 UpdateFinalRewards: ENTRY - epoch=ZZZZ+5, dataMarket=..., day=X, eligibleNodes=..., slotCount=...
✅ Step 1 complete: Updated eligible nodes count (...) for day X
✅ Successfully sent all ... batches for step 2 (final rewards update)...
```

## Quick Verification Commands

### Check Current Day from Contract
```bash
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "dayCounter()(uint256)" \
  --rpc-url https://rpc-devnet.powerloom.dev
```

### Check Eligible Nodes for Previous Day (after transition)
```bash
# Replace X with the previous day number
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "eligibleNodesForDay(uint256)(uint256)" X \
  --rpc-url https://rpc-devnet.powerloom.dev
```

### Check Redis Markers
```bash
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SMEMBERS "DayRolloverEpochMarkerSet.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
```

## Troubleshooting

### If Day Changes But No Transition Detected
- Check `CheckDayTransition` logs for comparison details
- Verify `lastKnownDay` is being persisted correctly

### If Transition Detected But No Buffer Epoch Match
- Check Redis markers exist: `SMEMBERS` command above
- Verify `bufferEpoch` calculation: should be `transitionEpoch + 5`
- Check if epoch numbers match

### If Buffer Epoch Reached But UpdateFinalRewards Not Called
- Check for "UpdateFinalRewards: ENTRY" logs
- If no entry logs, `IsBufferEpoch` is returning false
- Check buffer epoch matching logs for details

### If UpdateFinalRewards Called But Exits Early
- Check for "exiting early" messages
- Verify contract updates are enabled: `ENABLE_CONTRACT_UPDATES=true`
- Check if submissions exist for the day

## Success Criteria

✅ **Success indicators:**
1. Day transition detected when day changes
2. Marker created with correct buffer epoch
3. Buffer epoch matched 5 epochs later
4. UpdateFinalRewards called successfully
5. Contract shows eligible nodes count for previous day

❌ **Failure indicators:**
1. Day changes but no transition detected
2. Transition detected but no marker created
3. Marker exists but buffer epoch never matched
4. Buffer epoch matched but UpdateFinalRewards not called
5. UpdateFinalRewards called but contract not updated

## Quick Status Check Script

Save as `check_status.sh`:
```bash
#!/bin/bash

DATA_MARKET="0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
CONTAINER="libp2p-gossipsub-topic-debugger-p2p-debugger-1"
REDIS_CONTAINER="libp2p-gossipsub-topic-debugger-redis-1"

echo "=== Current Day (Contract) ==="
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "dayCounter()(uint256)" \
  --rpc-url https://rpc-devnet.powerloom.dev 2>/dev/null | xargs printf "%d\n"

echo ""
echo "=== Recent Day Changes ==="
sudo docker logs $CONTAINER -n 500 | grep -E "Day changed|Day unchanged" | tail -5

echo ""
echo "=== Recent Transitions ==="
sudo docker logs $CONTAINER -n 500 | grep "Day transition detected" | tail -3

echo ""
echo "=== Redis Markers ==="
sudo docker exec $REDIS_CONTAINER redis-cli SMEMBERS "DayRolloverEpochMarkerSet.$DATA_MARKET" 2>/dev/null

echo ""
echo "=== Recent Buffer Epoch Checks ==="
sudo docker logs $CONTAINER -n 500 | grep "IsBufferEpoch" | tail -5

echo ""
echo "=== UpdateFinalRewards Calls ==="
sudo docker logs $CONTAINER -n 10000 | grep "UpdateFinalRewards: ENTRY" | tail -3
```

Make executable and run:
```bash
chmod +x check_status.sh
./check_status.sh
```
