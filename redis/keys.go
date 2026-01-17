package redis

import (
	"fmt"
	"strings"
)

// SlotSubmissionKey returns the Redis key for slot submission count
// Format: SlotSubmissions.{dataMarket}.{day}.{slotID}
func SlotSubmissionKey(dataMarketAddress string, slotID string, day string) string {
	return fmt.Sprintf("SlotSubmissions.%s.%s.%s", strings.ToLower(dataMarketAddress), day, slotID)
}

// EligibleSlotSubmissionKey returns the Redis key for eligible slot submission count
// Format: EligibleSlotSubmissions.{dataMarket}.{day}.{slotID}
func EligibleSlotSubmissionKey(dataMarketAddress string, slotID string, day string) string {
	return fmt.Sprintf("EligibleSlotSubmissions.%s.%s.%s", strings.ToLower(dataMarketAddress), day, slotID)
}

// EligibleNodesByDayKey returns the Redis key for eligible nodes set for a day
// Format: EligibleNodesByDay.{dataMarket}.{day}
func EligibleNodesByDayKey(dataMarketAddress string, day string) string {
	return fmt.Sprintf("EligibleNodesByDay.%s.%s", strings.ToLower(dataMarketAddress), day)
}

// EligibleSlotSubmissionsByEpochKey returns the Redis key for eligible slot submissions by epoch
// Format: EligibleSlotSubmissionsByEpoch.{dataMarket}.{day}.{epochID}
func EligibleSlotSubmissionsByEpochKey(dataMarketAddress string, day string, epochID string) string {
	return fmt.Sprintf("EligibleSlotSubmissionsByEpoch.%s.%s.%s", strings.ToLower(dataMarketAddress), day, epochID)
}

// CurrentDayKey returns the Redis key for current day
// Format: CurrentDay.{dataMarket}
func CurrentDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("CurrentDay.%s", strings.ToLower(dataMarketAddress))
}

// LastKnownDayKey returns the Redis key for last known day
// Format: LastKnownDay.{dataMarket}
func LastKnownDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("LastKnownDay.%s", strings.ToLower(dataMarketAddress))
}

// DayRolloverEpochMarkerSet returns the Redis key for the set of epoch IDs with day transitions
// Format: DayRolloverEpochMarkerSet.{dataMarket}
func DayRolloverEpochMarkerSet(dataMarketAddress string) string {
	return fmt.Sprintf("DayRolloverEpochMarkerSet.%s", strings.ToLower(dataMarketAddress))
}

// DayRolloverEpochMarkerDetails returns the Redis key for day transition marker details
// Format: DayRolloverEpochMarkerDetails.{dataMarket}.{epochID}
func DayRolloverEpochMarkerDetails(dataMarketAddress string, epochID string) string {
	return fmt.Sprintf("DayRolloverEpochMarkerDetails.%s.%s", strings.ToLower(dataMarketAddress), epochID)
}

// DailySnapshotQuotaTableKey returns the Redis key for the daily snapshot quota hash table
// Format: DailySnapshotQuotaTableKey
func DailySnapshotQuotaTableKey() string {
	return "DailySnapshotQuotaTableKey"
}
