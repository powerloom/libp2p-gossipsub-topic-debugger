# Relayer Monitoring Commands

## Overview
Monitor relayer logs to debug why Step 2 (final rewards) isn't distributing rewards.

## Key Log Patterns

### 1. Monitor All Step 2 Related Logs (Comprehensive)
```bash
# Docker container name may vary - adjust as needed
sudo docker logs <relayer-container-name> -f | grep -E \
  "update eligible submission counts|UpdateEligibleSubmissionCounts|Queued update eligible submission counts|Error submitting update eligible submission counts|Gas estimation failed.*update eligible submission counts"
```

### 2. Monitor Step 2 Message Reception
```bash
# Check if relayer is receiving Step 2 requests
sudo docker logs <relayer-container-name> -f | grep -E \
  "Received update eligible submission counts request|update eligible submission counts request"
```

### 3. Monitor Step 2 Transaction Queueing
```bash
# Check if transactions are being queued
sudo docker logs <relayer-container-name> -f | grep -E \
  "Queued update eligible submission counts transaction|update eligible submission counts transaction"
```

### 4. Monitor Step 2 Errors
```bash
# Check for any errors in Step 2 processing
sudo docker logs <relayer-container-name> -f | grep -E \
  "Error submitting update eligible submission counts|Gas estimation failed.*update eligible submission counts|Contract logic error.*update eligible submission counts"
```

### 5. Monitor All End-of-Day Updates (Step 1 + Step 2)
```bash
# Monitor both Step 1 and Step 2
sudo docker logs <relayer-container-name> -f | grep -E \
  "update eligible nodes|update eligible submission counts|UpdateEligibleNodes|UpdateEligibleSubmissionCounts"
```

### 6. Monitor Transaction Processing
```bash
# Check if transactions are being processed/executed
sudo docker logs <relayer-container-name> -f | grep -E \
  "Processing.*update eligible submission counts|Executing.*update eligible submission counts|Transaction.*update eligible submission counts.*success|Transaction.*update eligible submission counts.*failed"
```

### 7. Monitor RabbitMQ Message Consumption
```bash
# Check if messages are being consumed from RabbitMQ
sudo docker logs <relayer-container-name> -f | grep -E \
  "Parsed message as UpdateEligibleSubmissionCountsRequest|Processing.*UpdateEligibleSubmissionCountsRequest"
```

### 8. Monitor Gas Estimation Failures
```bash
# Check for gas estimation errors (contract call failures)
sudo docker logs <relayer-container-name> -f | grep -E \
  "Gas estimation failed|Contract logic error|estimate_gas.*failed"
```

## Comprehensive Message Routing & Processing Monitor

### Monitor MessageType-Based Routing (NEW - After messageType field addition)
```bash
sudo docker logs p2p-debugger-relayer-py -f | grep -E \
  "tx_worker|Parsed message|messageType|UpdateEligibleSubmissionCounts|UpdateSubmissionCounts|UpdateEligibleNodes|Queued.*transaction|Processing transaction|Error submitting"
```

### Detailed Message Routing Monitor (Shows messageType routing)
```bash
sudo docker logs p2p-debugger-relayer-py -f | grep --line-buffered -E \
  "Parsed message as|messageType.*=|Queued.*transaction|Processing transaction|Error submitting|Could not identify message type"
```

### Step 2 Specific Monitor (With MessageType Routing)
```bash
sudo docker logs p2p-debugger-relayer-py -f | grep --line-buffered -E \
  "UpdateEligibleSubmissionCounts|messageType.*UpdateEligibleSubmissionCounts|Parsed message as UpdateEligibleSubmissionCountsRequest|Queued update eligible submission counts|Processing.*update eligible submission counts|Error.*update eligible submission counts"
```

## Expected Log Sequence for Step 2

### Step 1: Relayer Receives Request
```
Received update eligible submission counts request: UpdateEligibleSubmissionCountsRequest(...)
```

### Step 2: Message Published to RabbitMQ
```
Submitted Update Eligible Submission Counts to relayer!
```

### Step 3: Transaction Worker Consumes Message
```
Parsed message as UpdateEligibleSubmissionCountsRequest
```

### Step 4: Gas Estimation
```
(No logs if successful, or error logs if failed)
```

### Step 5: Transaction Queued
```
Queued update eligible submission counts transaction {tx_id}
```

### Step 6: Transaction Executed
```
(Check blockchain for transaction hash)
```

## Quick Diagnostic Commands

### Check if Relayer is Running
```bash
sudo docker ps | grep relayer
```

### Check Recent Step 2 Activity
```bash
sudo docker logs p2p-debugger-relayer-py --tail 1000 | grep -i "update eligible submission counts" | tail -20
```

### Check for Any Errors in Last Hour
```bash
sudo docker logs p2p-debugger-relayer-py --since 1h | grep -i "error.*update eligible submission counts"
```

### Monitor Real-time Step 2 Activity
```bash
sudo docker logs p2p-debugger-relayer-py -f | grep --line-buffered -E \
  "update eligible submission counts|UpdateEligibleSubmissionCounts"
```

## What to Look For

### ✅ Good Signs
- `Received update eligible submission counts request` - Relayer received request
- `Parsed message as UpdateEligibleSubmissionCountsRequest` - MessageType routing worked
- `Queued update eligible submission counts transaction {tx_id}` - Transaction queued
- Transaction hash appears in logs (means transaction was sent)

### ❌ Bad Signs
- No logs at all for Step 2 - Messages not reaching relayer
- `Error submitting update eligible submission counts` - Processing failed
- `Gas estimation failed` - Contract call would fail
- `Contract logic error` - Contract validation failed
- `Could not identify message type` - MessageType parsing failed
- Transaction queued but never executed - Transaction queue issue

## Combined Monitoring (Go + Relayer)

### Terminal 1: Monitor Go Code
```bash
sudo docker logs libp2p-gossipsub-topic-debugger-p2p-debugger-1 -f | grep -E \
  "Step 2|update eligible submission counts|UpdateEligibleSubmissionCounts"
```

### Terminal 2: Monitor Relayer (MessageType Routing)
```bash
sudo docker logs p2p-debugger-relayer-py -f | grep --line-buffered -E \
  "tx_worker|Parsed message|messageType|UpdateEligibleSubmissionCounts|Queued.*update eligible|Processing transaction|Error submitting"
```

## Troubleshooting

### If No Relayer Logs Appear:
1. Check if relayer container is running: `sudo docker ps | grep relayer`
2. Check if messages are reaching RabbitMQ
3. Check if transaction worker is consuming messages

### If Transaction Queued But Not Executed:
1. Check transaction queue status
2. Check if relayer has enough gas/balance
3. Check blockchain for transaction status using tx_id

### If Gas Estimation Fails:
1. Check contract state - is `eligibleNodesForDay[day]` set?
2. Check if data market address is correct
3. Check if slot IDs and submission counts arrays match length

### If MessageType Parsing Fails:
1. Check if `messageType` field is present in message JSON
2. Check logs for "Could not identify message type" error
3. Verify messageType value matches expected values
