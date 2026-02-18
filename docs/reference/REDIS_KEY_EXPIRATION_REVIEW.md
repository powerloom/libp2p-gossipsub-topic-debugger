# Redis Key Expiration Review

## Summary

Review of all Redis keys used in the submission counter and day transition manager to identify which keys need expiration/TTL.

## Redis Keys Used

### 1. SlotSubmissionKey
- **Format**: `SlotSubmissions.{dataMarket}.{day}.{slotID}`
- **Usage**: Accumulated submission count per slot per day
- **Current Expiration**: ❌ None
- **Should Expire**: ✅ Yes, after final rewards processed (keep for safety period)
- **Status**: Needs expiration after final rewards

### 2. EligibleSlotSubmissionKey
- **Format**: `EligibleSlotSubmissions.{dataMarket}.{day}.{slotID}`
- **Usage**: Eligible submission count per slot per day
- **Current Expiration**: ❌ None
- **Should Expire**: ✅ Yes, after final rewards processed (keep for safety period)
- **Status**: Needs expiration after final rewards

### 3. EligibleNodesByDayKey
- **Format**: `EligibleNodesByDay.{dataMarket}.{day}`
- **Usage**: Set of slot IDs with submissions for a day
- **Current Expiration**: ❌ None
- **Should Expire**: ✅ Yes, after final rewards processed (keep for safety period)
- **Status**: Needs expiration after final rewards

### 4. EligibleSlotSubmissionsByEpochKey ⚠️ TRANSIENT
- **Format**: `EligibleSlotSubmissionsByEpoch.{dataMarket}.{day}.{epochID}`
- **Usage**: Epoch-specific tracking (debugging only, NOT used for final rewards)
- **Current Expiration**: ✅ **FIXED** - Now expires after 48 hours
- **Should Expire**: ✅ Yes (transient data)
- **Status**: ✅ Fixed - Added 48-hour expiration

### 5. DayRolloverEpochMarkerSet
- **Format**: `DayRolloverEpochMarkerSet.{dataMarket}`
- **Usage**: Set of epoch IDs with day transitions
- **Current Expiration**: ❌ None (set itself doesn't expire, but members are removed)
- **Should Expire**: ⚠️ Members removed manually, set could accumulate
- **Status**: Members removed, but set could grow - consider cleanup

### 6. DayRolloverEpochMarkerDetails
- **Format**: `DayRolloverEpochMarkerDetails.{dataMarket}.{epochID}`
- **Usage**: Day transition marker details
- **Current Expiration**: ✅ **30 minutes** after marker removal
- **Should Expire**: ✅ Yes (transient)
- **Status**: ✅ Already has expiration

### 7. LastKnownDayKey
- **Format**: `LastKnownDay.{dataMarket}`
- **Usage**: Last known day for a data market
- **Current Expiration**: ❌ None
- **Should Expire**: ⚠️ Probably keep (persistent state)
- **Status**: OK to keep (persistent state)

### 8. CurrentDayKey
- **Format**: `CurrentDay.{dataMarket}`
- **Usage**: Current day cache
- **Current Expiration**: ❌ None (if used)
- **Should Expire**: ⚠️ Probably keep (persistent state)
- **Status**: OK to keep (persistent state)

## Changes Made

### ✅ Fixed: EligibleSlotSubmissionsByEpochKey
Added 48-hour expiration to epoch-specific keys (transient tracking data):
```go
if err := redis.HSet(sc.ctx, epochKey, slotIDStr, strconv.FormatInt(newCount, 10)); err != nil {
    // ...
} else {
    // Set expiration on epoch-specific keys (transient data, expire after 2 days)
    if err := redis.Expire(sc.ctx, epochKey, 48*time.Hour); err != nil {
        log.Printf("⚠️ Failed to set expiration on epoch key %s: %v", epochKey, err)
    }
}
```

## Recommendations

### High Priority: Day-Based Keys After Final Rewards

After `UpdateFinalRewards` succeeds and `ResetCountsForDay` is called, we should also expire the Redis keys for that day. Currently `ResetCountsForDay` only clears in-memory cache.

**Suggested fix**: Add Redis key expiration/deletion in `ResetCountsForDay` or after final rewards processing.

### Medium Priority: DayRolloverEpochMarkerSet Cleanup

The set itself doesn't expire, only individual markers. Consider periodic cleanup of old epoch IDs from the set.

## Key Categories

### Persistent Keys (Keep)
- `LastKnownDayKey` - Persistent state
- `CurrentDayKey` - Persistent state (if used)

### Transient Keys (Should Expire)
- ✅ `EligibleSlotSubmissionsByEpochKey` - **FIXED** (48 hours)
- ✅ `DayRolloverEpochMarkerDetails` - Already expires (30 minutes)

### Day-Based Keys (Expire After Final Rewards)
- `SlotSubmissionKey` - Needs expiration after final rewards
- `EligibleSlotSubmissionKey` - Needs expiration after final rewards  
- `EligibleNodesByDayKey` - Needs expiration after final rewards

These should expire after final rewards are processed (maybe keep for 7 days for safety/audit).
