# Contract Queries: Eligible Nodes for a Day

## Overview
This document describes how to query eligible nodes count for a specific day from the DataMarket contract.

## DataMarket Contract Query

### Function Signature
```solidity
function eligibleNodesForDay(uint256 dayId) public view returns(uint256 eligibleNodes)
```

### Contract Details
- **Contract**: `DataMarket` contract
- **Address**: `0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371` (BDS DSV Devnet)
- **Method ID**: `0x3b54c84e`
- **Type**: Public view function (read-only)

### How It Works
The `eligibleNodesForDay` mapping is declared as `public` in the DataMarket contract:
```solidity
mapping(uint256 dayId => uint256 eligibleNodes) public eligibleNodesForDay;
```

In Solidity, public mappings automatically generate getter functions, so you can call `eligibleNodesForDay(day)` to get the count.

### Query Methods

#### 1. Using cast (Foundry)
```bash
# Query eligible nodes for day 65
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "eligibleNodesForDay(uint256)(uint256)" \
  65 \
  --rpc-url https://rpc-devnet.powerloom.dev

# Expected output: uint256 value (e.g., 1000)
```

#### 2. Using web3.py (Python)
```python
from web3 import Web3

# Connect to RPC
w3 = Web3(Web3.HTTPProvider('https://rpc-devnet.powerloom.dev'))

# DataMarket contract address
data_market_address = '0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371'

# ABI for the function (minimal ABI)
abi = [{
    "inputs": [{"internalType": "uint256", "name": "dayId", "type": "uint256"}],
    "name": "eligibleNodesForDay",
    "outputs": [{"internalType": "uint256", "name": "eligibleNodes", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function"
}]

# Create contract instance
contract = w3.eth.contract(address=Web3.to_checksum_address(data_market_address), abi=abi)

# Query for day 65
day = 65
eligible_nodes = contract.functions.eligibleNodesForDay(day).call()

print(f"Eligible nodes for day {day}: {eligible_nodes}")
```

#### 3. Using ethers.js (JavaScript)
```javascript
const { ethers } = require("ethers");

// Connect to RPC
const provider = new ethers.JsonRpcProvider("https://rpc-devnet.powerloom.dev");

// DataMarket contract address
const dataMarketAddress = "0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371";

// ABI for the function
const abi = [
  {
    inputs: [{ internalType: "uint256", name: "dayId", type: "uint256" }],
    name: "eligibleNodesForDay",
    outputs: [{ internalType: "uint256", name: "eligibleNodes", type: "uint256" }],
    stateMutability: "view",
    type: "function"
  }
];

// Create contract instance
const contract = new ethers.Contract(dataMarketAddress, abi, provider);

// Query for day 65
const day = 65;
const eligibleNodes = await contract.eligibleNodesForDay(day);

console.log(`Eligible nodes for day ${day}: ${eligibleNodes.toString()}`);
```

#### 4. Using Go (ethereum/go-ethereum)
```go
package main

import (
    "context"
    "fmt"
    "math/big"
    
    "github.com/ethereum/go-ethereum/accounts/abi/bind"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethclient"
)

func main() {
    // Connect to RPC
    client, err := ethclient.Dial("https://rpc-devnet.powerloom.dev")
    if err != nil {
        panic(err)
    }
    
    // DataMarket contract address
    dataMarketAddress := common.HexToAddress("0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371")
    
    // Create contract binding (you'll need the full ABI)
    // For now, using direct call:
    callOpts := &bind.CallOpts{Context: context.Background()}
    
    // You would use the generated contract bindings:
    // count, err := dataMarketContract.EligibleNodesForDay(callOpts, big.NewInt(65))
    
    fmt.Printf("Eligible nodes for day 65: %d\n", count.Uint64())
}
```

#### 5. Direct RPC Call (curl)
```bash
# Using eth_call
curl -X POST https://rpc-devnet.powerloom.dev \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_call",
    "params": [{
      "to": "0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371",
      "data": "0x3b54c84e0000000000000000000000000000000000000000000000000000000000000041"
    }, "latest"],
    "id": 1
  }'

# Where:
# - "0x3b54c84e" is the method ID for eligibleNodesForDay(uint256)
# - "0000000000000000000000000000000000000000000000000000000000000041" is day 65 in hex (0x41)
```

### Return Value
- **Type**: `uint256`
- **Meaning**: Number of eligible nodes (slots) for the specified day
- **Example**: `1000` means 1000 slots were eligible for rewards on that day

### Important Notes

1. **Zero vs Not Set**: 
   - Returns `0` if the day hasn't been processed yet (no eligible nodes set)
   - Returns `0` if no nodes were eligible for that day
   - The value is set by `updateEligibleNodesForDay()` which is called during final rewards update

2. **Day Validation**:
   - The day must be a valid day (typically `dayCounter` or `dayCounter - 1`)
   - Querying future days will return `0` (not set yet)

3. **When Value is Set**:
   - The value is set when `UpdateFinalRewards` is called via `updateEligibleNodesForDay()`
   - This happens at the buffer epoch after a day transition
   - Once set, it shouldn't change (unless there's an error condition)

### Related Contract Functions

#### Get Current Day
```solidity
function dayCounter() public view returns(uint256)
```

Query current day:
```bash
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "dayCounter()(uint256)" \
  --rpc-url https://rpc-devnet.powerloom.dev
```

#### Get Slot Submission Count for a Day
```solidity
function slotSubmissionCount(uint256 slotId, uint256 dayId) public view returns(uint256)
```

Query slot submission count:
```bash
cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
  "slotSubmissionCount(uint256,uint256)(uint256)" \
  <slotId> <dayId> \
  --rpc-url https://rpc-devnet.powerloom.dev
```

### ProtocolState Contract

The ProtocolState contract does NOT have a direct wrapper for this function. You must query the DataMarket contract directly.

However, ProtocolState does have a function to get slot submission count:
```solidity
function slotSubmissionCount(PowerloomDataMarket dataMarket, uint256 slotId, uint256 dayId) public view returns (uint256)
```

But for eligible nodes count, query DataMarket directly.

### Debugging Use Cases

1. **Verify if final rewards were processed**:
   ```bash
   # If this returns 0, final rewards haven't been processed for day 65
   cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
     "eligibleNodesForDay(uint256)(uint256)" 65 \
     --rpc-url https://rpc-devnet.powerloom.dev
   ```

2. **Compare with expected value**:
   - If you expect 1000 eligible nodes but contract shows 0, final rewards weren't updated
   - If contract shows a different value, check if counts were calculated correctly

3. **Check multiple days**:
   ```bash
   for day in 60 61 62 63 64 65; do
     echo "Day $day:"
     cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
       "eligibleNodesForDay(uint256)(uint256)" $day \
       --rpc-url https://rpc-devnet.powerloom.dev
   done
   ```

### Example Output Interpretation

```bash
# Query day 65
$ cast call 0xF7F21D06894FC378B7E347a6132F8fE1e4f0F371 \
    "eligibleNodesForDay(uint256)(uint256)" 65 \
    --rpc-url https://rpc-devnet.powerloom.dev
0x00000000000000000000000000000000000000000000000000000000000003e8

# Convert hex to decimal: 0x3e8 = 1000
# Result: 1000 eligible nodes for day 65
```

If result is `0x0000000000000000000000000000000000000000000000000000000000000000`:
- Day hasn't been processed yet (UpdateFinalRewards not called)
- OR no nodes were eligible for that day
