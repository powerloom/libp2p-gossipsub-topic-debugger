# Querying Stored Eligible Nodes from Redis

## Overview
Eligible nodes are stored per day in Redis. Here's how to query them.

## Redis Key Structure

### Main Key: Eligible Nodes Set
```
EligibleNodesByDay.{dataMarket}.{day}
```
This is a Redis SET containing all slot IDs that have submissions for that day.

### Individual Slot Counts
```
EligibleSlotSubmissions.{dataMarket}.{day}.{slotID}
SlotSubmissions.{dataMarket}.{day}.{slotID}
```

## Query Commands

### 1. List All Eligible Slot IDs for a Day

```bash
# Get all slot IDs in the eligible nodes set for day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SMEMBERS "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"
```

**Output**: List of slot ID strings (e.g., `["1234", "5678", "9012"]`)

### 2. Count Eligible Nodes for a Day

```bash
# Count how many slots are eligible for day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SCARD "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"
```

**Output**: Integer count (e.g., `1000`)

### 3. Get Submission Count for a Specific Slot on a Day

```bash
# Get eligible submission count for slot 1234 on day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  GET "EligibleSlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234"

# Or fallback to SlotSubmissions key
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  GET "SlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234"
```

**Output**: Count as string (e.g., `"30"`)

### 4. Get All Slot Counts for a Day (Bash Script)

```bash
#!/bin/bash

DATA_MARKET="0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
DAY="65"
CONTAINER="libp2p-gossipsub-topic-debugger-redis-1"

# Get all slot IDs
SLOT_IDS=$(sudo docker exec $CONTAINER redis-cli \
  SMEMBERS "EligibleNodesByDay.$DATA_MARKET.$DAY")

echo "Eligible nodes for day $DAY:"
echo "Total slots: $(echo "$SLOT_IDS" | grep -v '^$' | wc -l)"
echo ""

# Get count for each slot
for slot_id in $SLOT_IDS; do
  if [ -n "$slot_id" ]; then
    count=$(sudo docker exec $CONTAINER redis-cli \
      GET "EligibleSlotSubmissions.$DATA_MARKET.$DAY.$slot_id" 2>/dev/null)
    if [ -z "$count" ]; then
      count=$(sudo docker exec $CONTAINER redis-cli \
        GET "SlotSubmissions.$DATA_MARKET.$DAY.$slot_id" 2>/dev/null)
    fi
    echo "Slot $slot_id: $count submissions"
  fi
done
```

### 5. Check if a Specific Slot is Eligible

```bash
# Check if slot 1234 is in the eligible nodes set for day 65
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  SISMEMBER "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65" "1234"
```

**Output**: `1` if member, `0` if not

### 6. List All Days with Eligible Nodes

```bash
# Find all EligibleNodesByDay keys (shows all days)
sudo docker exec -it libp2p-gossipsub-topic-debugger-redis-1 redis-cli \
  KEYS "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.*"
```

**Output**: List of keys like:
```
1) "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.64"
2) "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"
3) "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.66"
```

### 7. Get Summary for Multiple Days

```bash
#!/bin/bash

DATA_MARKET="0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
CONTAINER="libp2p-gossipsub-topic-debugger-redis-1"

# Get all day keys
DAYS=$(sudo docker exec $CONTAINER redis-cli \
  KEYS "EligibleNodesByDay.$DATA_MARKET.*" | \
  sed 's/.*\.\([0-9]*\)$/\1/')

echo "Day | Eligible Nodes Count"
echo "----|---------------------"

for day in $DAYS; do
  count=$(sudo docker exec $CONTAINER redis-cli \
    SCARD "EligibleNodesByDay.$DATA_MARKET.$day" 2>/dev/null)
  echo "$day | $count"
done
```

### 8. Get Detailed Info for a Day (Python Script)

```python
#!/usr/bin/env python3
import redis
import sys

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

data_market = "0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371"
day = sys.argv[1] if len(sys.argv) > 1 else "65"

# Get eligible nodes set key
set_key = f"EligibleNodesByDay.{data_market}.{day}"

# Get all slot IDs
slot_ids = r.smembers(set_key)
print(f"Day {day}: {len(slot_ids)} eligible slots\n")

# Get count for each slot
slot_counts = {}
for slot_id in slot_ids:
    eligible_key = f"EligibleSlotSubmissions.{data_market}.{day}.{slot_id}"
    count = r.get(eligible_key)
    if not count:
        slot_key = f"SlotSubmissions.{data_market}.{day}.{slot_id}"
        count = r.get(slot_key)
    slot_counts[slot_id] = int(count) if count else 0

# Sort by count (descending)
sorted_slots = sorted(slot_counts.items(), key=lambda x: x[1], reverse=True)

print("Top 10 slots by submission count:")
for slot_id, count in sorted_slots[:10]:
    print(f"  Slot {slot_id}: {count} submissions")

print(f"\nTotal submissions: {sum(slot_counts.values())}")
```

## Quick Reference

### Key Patterns

| What | Redis Key Pattern | Type |
|------|-------------------|------|
| Eligible nodes set | `EligibleNodesByDay.{dataMarket}.{day}` | SET |
| Slot submission count | `SlotSubmissions.{dataMarket}.{day}.{slotID}` | STRING |
| Eligible slot count | `EligibleSlotSubmissions.{dataMarket}.{day}.{slotID}` | STRING |
| Epoch-specific (transient) | `EligibleSlotSubmissionsByEpoch.{dataMarket}.{day}.{epochID}` | HASH |

### Common Queries

```bash
# Quick check: How many eligible nodes for day 65?
redis-cli SCARD "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"

# List all slot IDs for day 65
redis-cli SMEMBERS "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65"

# Get count for slot 1234 on day 65
redis-cli GET "EligibleSlotSubmissions.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.65.1234"

# Check all days with data
redis-cli KEYS "EligibleNodesByDay.0xf7f21d06894fc378b7e347a6132f8fe1e4f0f371.*"
```

## Notes

1. **EligibleNodesByDay set**: Contains all slot IDs that have submissions (>0) for that day
2. **Final eligibility**: The set includes all slots with submissions, but final eligibility (>= quota) is checked at final rewards time
3. **Counts accumulate**: Slot counts accumulate across epochs within the same day
4. **Keys are per day**: All keys include the day, so data is segregated by day
