# libp2p-gossipsub-topic-debugger

A utility for debugging and monitoring libp2p gossipsub topics, with extended functionality for validator mesh submission count tracking and on-chain updates.

## Overview

This utility serves two primary purposes:

1. **Debugging Tool**: Monitor and debug libp2p gossipsub topics in the Powerloom network
2. **Validator Mesh Submission Counter**: Track finalized batches from DSV (Decentralized Sequencer-Validator) nodes, aggregate them using consensus logic, and optionally update protocol contracts with eligible submission counts

## Why This Exists

During the transition from centralized sequencer to decentralized sequencer-validator (DSV) mesh:

- **Legacy System**: The centralized `submission-sequencer-event-collector` periodically updated eligible submission counts on the protocol contract
- **New System**: DSV nodes perform Level 2 aggregations and commit them via VPA (Validator Priority Assignment), but they don't update submission counts or end-of-day reward updates
- **Transition Solution**: This utility independently joins the validator mesh, listens to finalized batches from all validators, aggregates them using consensus logic, and updates the protocol contract - bridging the gap during the transition

**Important Notes**:
- **Level 1 vs Level 2 Batches**: The validator mesh (`/powerloom/finalized-batches/all`) only receives **Level 1 finalizations** (individual validator's local batches). Level 2 aggregated batches are created locally by each validator and are NOT broadcast back to the network. This utility performs its own Level 2 aggregation from the Level 1 batches it receives.
- **Data Market Information**: Currently, Level 1 batches don't contain `dataMarket` information. This utility uses a single configured `DATA_MARKET_ADDRESS` from environment variables. DSV nodes should add `dataMarket` to Level 1 batches in the future.

## Architecture

### Validator Mesh Mode

When `VALIDATOR_MESH_MODE=true`, the utility:

1. **Joins Validator Mesh**: Connects to validator-specific gossipsub topics (not snapshotter mesh)
2. **Monitors Epochs**: Listens for `EpochReleased` events from the protocol state contract
3. **Collects Batches**: Receives finalized batches from all validators during the aggregation window
4. **Applies Consensus**: Aggregates batches using majority vote logic per project/CID
5. **Extracts Counts**: Calculates eligible submission counts per slot ID
6. **Generates Tallies**: Creates per-epoch JSON dumps with rotation/pruning
7. **Updates Contract**: Optionally updates protocol contract every N epochs

### Two-Phase Aggregation Window

The utility honors the DSV aggregation window timing:

```
EpochReleased Event Detected
  ↓
Wait LEVEL1_FINALIZATION_DELAY_SECONDS (10s default)
  [DSV nodes complete internal Level 1 aggregation]
  ↓
Start Accepting Validator Batches
  ↓
Start Aggregation Window Timer (AGGREGATION_WINDOW_SECONDS = 20s default)
  [Collect batches from all validators during this window]
  ↓
Window Expires → Finalize Tally
  ↓
Generate Tally Dump (JSON file + stdout)
  ↓
[Optional] Update Contract (if enabled and epoch matches interval)
```

**Phase 1 - Level 1 Finalization Delay**:
- DSV nodes complete their internal Level 1 aggregation
- The utility waits and does NOT accept batches yet
- Ensures all validators have completed their local processing

**Phase 2 - Aggregation Window**:
- Validators start broadcasting their Level 1 finalizations (individual validator batches)
- The utility collects Level 1 batches from all validators
- After the window expires, the utility performs Level 2 aggregation (consensus) across all received Level 1 batches
- Note: Each validator also performs Level 2 aggregation locally, but those aggregated batches are NOT broadcast

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- Access to libp2p bootstrap peers
- (Optional) RPC URL for event monitoring
- (Optional) Protocol state contract address

### Docker Setup (Recommended)

The docker-compose setup includes:
- `p2p-debugger`: The main debugger service
- `relayer-py`: Python-based transaction relayer for contract updates
- `redis`: Required by relayer-py and p2p-debugger for state management
- `rabbitmq`: Required by relayer-py for transaction queueing

#### Setup Instructions

1. **Configure environment variables**:
```bash
cp .env.example .env
# Edit .env with your configuration:
# - Set RELAYER_URL=http://relayer-py:8080 for docker-compose
# - Configure VPA_SIGNER_ADDRESSES and VPA_SIGNER_PRIVATE_KEYS for relayer-py
# - Set POWERLOOM_RPC_NODES (comma-separated list)
# - Set DATA_MARKET_ADDRESS
# - Set PROTOCOL_STATE_CONTRACT
# - Set RELAYER_PY_IMAGE_TAG (defaults to "latest")
```

2. **Start services** (automatically handles dev/production):

   **Development (Build from Source)**:
   ```bash
   ./bootstrap.sh  # Clones relayer-py repository (optional - start.sh handles this)
   ./start.sh      # Automatically detects relayer-py and builds from source
   ```

   **Production (Use Pre-built Image)**:
   ```bash
   ./start.sh  # Automatically detects missing relayer-py and uses pre-built image
   ```

   The `start.sh` script automatically:
   - Checks if `./relayer-py` directory exists
   - If exists: builds from source (development mode)
   - If missing: creates override file and uses pre-built image (production mode)
   - No manual configuration needed

3. **Configure Trusted Updaters** (REQUIRED for contract updates):
   - The signer addresses in `VPA_SIGNER_ADDRESSES` must be added to ProtocolState contract's `trustedUpdaters` mapping
   - Call `ProtocolState.addTrustedUpdater(signerAddress)` from contract owner
   - Verify: `ProtocolState.trustedUpdaters(signerAddress)` returns `true`
   - See "Trusted Updaters Setup" section below for details

**How it works**:
- Docker-compose **merges** multiple compose files - it doesn't replace them
- The main `docker-compose.yml` defines ALL sections: `image`, `build`, `environment`, `depends_on`, `networks`, `ports`, `volumes`, `healthcheck`, etc.
- The override file (`docker-compose.override.yml`) only needs to specify what's different - docker-compose merges it with the base file
- When `./relayer-py` doesn't exist: `start.sh` creates override with `build: null` to remove build section, keeping all other sections from base file
- When `./relayer-py` exists: override file is removed, docker-compose builds from source using all sections from base file
- **Key point**: The override file doesn't need to duplicate environment, depends_on, networks, etc. - those are automatically merged from the base file

4. **View logs**:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f p2p-debugger
docker-compose logs -f relayer-py
```

5. **Check service health**:
```bash
# Check relayer-py health (from within Docker network)
docker-compose exec relayer-py curl http://localhost:8080/health

# Check Redis
docker-compose exec redis redis-cli ping

# Check RabbitMQ management UI
# Open http://localhost:15672 (default: guest/guest)
```

6. **Stop**:
```bash
./stop.sh
# Or manually:
docker-compose down
```

### Manual Setup (Alternative)

If you prefer to run without Docker:

1. **Build**:
```bash
go mod download
go build -o gossipsub-debugger .
```

2. **Run**:
```bash
./gossipsub-debugger
```

### Validator Mesh Mode Setup

To enable validator mesh mode and submission counting:

1. **Set validator mesh mode**:
```bash
export VALIDATOR_MESH_MODE=true
```

2. **Configure validator topics** (devnet defaults):
```bash
export GOSSIPSUB_FINALIZED_BATCH_PREFIX=/powerloom/dsv-devnet-alpha/finalized-batches
export GOSSIPSUB_VALIDATOR_PRESENCE_TOPIC=/powerloom/dsv-devnet-alpha/validator/presence
```

3. **Configure window timing**:
```bash
export LEVEL1_FINALIZATION_DELAY_SECONDS=10  # Wait for DSV Level 1 aggregation
export AGGREGATION_WINDOW_SECONDS=20         # Collect validator batches
```

4. **Configure data market** (REQUIRED - Level 1 batches don't contain dataMarket info):
```bash
export DATA_MARKET_ADDRESS=0xYourDataMarketAddress
```

5. **Enable event monitoring** (to track epoch windows):
```bash
export POWERLOOM_RPC_URL=https://your-powerloom-rpc-url-here
export PROTOCOL_STATE_CONTRACT=0xYourProtocolStateContractAddress
```

5. **Configure tally dumps**:
```bash
export ENABLE_TALLY_DUMPS=true
export TALLY_DUMP_DIR=./tallies
export TALLY_RETENTION_FILES=1000      # Keep last 1000 files
export TALLY_RETENTION_DAYS=7          # Or keep last 7 days
```

6. **Optional: Enable contract updates**:
```bash
export ENABLE_CONTRACT_UPDATES=true
export SUBMISSION_UPDATE_EPOCH_INTERVAL=10  # Update every 10 epochs

# Choose update method: "relayer" (recommended) or "direct"
export CONTRACT_UPDATE_METHOD=relayer

# For docker-compose: Use docker service name
export RELAYER_URL=http://relayer-py:8080
# For standalone: Use localhost
# export RELAYER_URL=http://localhost:8080

export RELAYER_AUTH_TOKEN=your_token_here

# Relayer-py configuration (for docker-compose)
export POWERLOOM_RPC_NODES=https://your-rpc-node-1,https://your-rpc-node-2
export VPA_SIGNER_ADDRESSES=0xYourSignerAddress1,0xYourSignerAddress2
export VPA_SIGNER_PRIVATE_KEYS=your_private_key_1,your_private_key_2
export AUTH_TOKEN=${RELAYER_AUTH_TOKEN}  # Same as RELAYER_AUTH_TOKEN

# OR for direct calls (not recommended with docker-compose):
export CONTRACT_UPDATE_METHOD=direct
export EVM_PRIVATE_KEY=your_evm_private_key_hex
```

### Trusted Updaters Setup

Before enabling contract updates, you must configure trusted updaters on the ProtocolState contract:

1. **Get signer addresses**: The addresses configured in `VPA_SIGNER_ADDRESSES` will be used to sign transactions

2. **Add to ProtocolState contract**: Call from contract owner:
```solidity
ProtocolState.addTrustedUpdater(signerAddress)
```

3. **Verify**: Check that the address is trusted:
```solidity
ProtocolState.trustedUpdaters(signerAddress) // Should return true
```

4. **Multiple signers**: You can add multiple signers for load balancing:
```bash
# In .env
VPA_SIGNER_ADDRESSES=0xSigner1,0xSigner2,0xSigner3
VPA_SIGNER_PRIVATE_KEYS=key1,key2,key3
```

**Important Notes**:
- Only addresses in `trustedUpdaters` can call `updateRewards`/`updateSubmissionCounts` functions
- The relayer-py service uses these signers to submit transactions
- Ensure signers have sufficient balance for gas fees
- Signers must be added before enabling contract updates

### Bootstrap Peers

You must provide bootstrap peers to connect to the network:

```bash
export BOOTSTRAP_PEERS=/ip4/1.2.3.4/tcp/4001/p2p/QmPeerID1,/ip4/5.6.7.8/tcp/4001/p2p/QmPeerID2
```

## How It Works

### 1. Event Monitoring

The `EventMonitor` watches for `EpochReleased` events from the protocol state contract:

- Polls blockchain at configurable intervals (default: 12 seconds)
- Parses events to extract: `epochID`, `dataMarketAddress`, `begin`, `end`, `timestamp`
- Filters by data market addresses if configured
- Triggers window management for each epoch

### 2. Window Management

The `WindowManager` tracks epoch windows per `(epochID, dataMarket)` combination:

- **On EpochReleased**: Creates a new window, starts Level 1 delay timer
- **After Level 1 Delay**: Transitions to "collecting batches" state
- **During Aggregation Window**: Accepts batches from validators
- **On Window Close**: Triggers tally finalization callback

### 3. Batch Processing

The `BatchProcessor` handles incoming validator batches:

- Parses `ValidatorBatch` messages from gossipsub topics
- Checks if batches can be accepted (must be past Level 1 delay)
- Groups batches by `epochID`
- Tracks first batch arrival per epoch
- Stores batches until aggregation window closes

### 4. Consensus Aggregation

When the aggregation window closes:

- Applies majority vote logic per project/CID
- Selects winning CID for each project (most votes)
- Merges `SubmissionDetails` from all validators
- Produces a single `FinalizedBatch` representing consensus

### 5. Submission Count Extraction

The `SubmissionCounter` extracts eligible counts:

- Iterates through aggregated `SubmissionDetails` (map[projectID][]SubmissionMetadata)
- Extracts `SlotID` from each `SubmissionMetadata`
- Groups by `(dataMarketAddress, slotID)`
- Counts submissions per slot
- Calculates eligible nodes count (unique slots with submissions)

### 6. Tally Dumps

The `TallyDumper` generates per-epoch reports:

- Creates JSON files: `epoch_{epochID}_{dataMarket}.json`
- Logs structured JSON to stdout
- Includes: epoch ID, data market, submission counts, eligible nodes, validator count
- Rotates/prunes old files based on retention policies

### 7. Contract Updates (Optional)

If `ENABLE_CONTRACT_UPDATES=true`:

- Checks if epoch matches update interval (default: every 10 epochs)
- Fetches current day from DataMarket contract using `dayCounter()` view function
- Sends update requests to relayer-py service via HTTP POST
- Relayer-py queues transactions and submits to ProtocolState contract
- Supports both relayer HTTP requests (recommended) and direct contract calls
- Implements exponential backoff retry logic

**Day Fetching**:
- Uses DataMarket contract's `dayCounter()` view function
- Requires `POWERLOOM_RPC_URL` to be set (even when using relayer method)
- Falls back to error if day fetch fails (no longer uses placeholder calculation)

**Docker Compose Integration**:
- When using docker-compose, `RELAYER_URL` should be `http://relayer-py:8080`
- Relayer-py service runs alongside debugger in same docker network
- Redis and RabbitMQ are automatically started as dependencies

## Configuration Reference

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `VALIDATOR_MESH_MODE` | `false` | Enable validator mesh mode |
| `BOOTSTRAP_PEERS` | *required* | Comma-separated bootstrap peer addresses |

### Validator Mesh Topics

| Variable | Default | Description |
|----------|---------|-------------|
| `GOSSIPSUB_FINALIZED_BATCH_PREFIX` | `/powerloom/dsv-devnet-alpha/finalized-batches` | Prefix for finalized batch topics |
| `GOSSIPSUB_VALIDATOR_PRESENCE_TOPIC` | `/powerloom/dsv-devnet-alpha/validator/presence` | Validator presence topic |

### Window Timing

| Variable | Default | Description |
|----------|---------|-------------|
| `LEVEL1_FINALIZATION_DELAY_SECONDS` | `10` | Wait time for DSV Level 1 aggregation |
| `AGGREGATION_WINDOW_SECONDS` | `20` | Time to collect validator batches |

### Tally Dumps

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_TALLY_DUMPS` | `true` | Enable per-epoch tally dumps |
| `TALLY_DUMP_DIR` | `./tallies` | Directory for JSON tally files |
| `TALLY_RETENTION_FILES` | `1000` | Keep last N files (0 = unlimited) |
| `TALLY_RETENTION_DAYS` | `7` | Keep files from last N days (0 = unlimited) |
| `TALLY_PRUNE_INTERVAL_HOURS` | `1` | Prune interval in hours |

### Event Monitoring

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_MARKET_ADDRESS` | *required* | Single data market address (Level 1 batches don't contain this info) |
| `POWERLOOM_RPC_URL` | *required* | Powerloom chain RPC URL for event monitoring |
| `PROTOCOL_STATE_CONTRACT` | *required* | Protocol state contract address |
| `EVENT_POLL_INTERVAL` | `12` | Poll interval in seconds |
| `EVENT_START_BLOCK` | `0` | Start block (0 = latest) |
| `EVENT_BLOCK_BATCH_SIZE` | `1000` | Block batch size for queries |

### Contract Updates

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_CONTRACT_UPDATES` | `false` | Enable/disable contract updates |
| `SUBMISSION_UPDATE_EPOCH_INTERVAL` | `10` | Update every N epochs |
| `CONTRACT_UPDATE_METHOD` | `relayer` | `relayer` or `direct` |
| `RELAYER_URL` | `http://relayer-py:8080` | Relayer service URL (use docker service name for docker-compose, `http://localhost:8080` for standalone) |
| `RELAYER_AUTH_TOKEN` | *required if relayer* | Relayer auth token (must match `AUTH_TOKEN` in relayer-py) |
| `EVM_PRIVATE_KEY` | *required if direct* | EVM private key hex (secp256k1) for direct contract calls. Note: This is different from `PRIVATE_KEY` which is Ed25519 for libp2p peer identity. |
| `POWERLOOM_RPC_URL` | *required* | RPC URL for day fetching from DataMarket contract (required even when using relayer method) |

### Docker Compose Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_PORT` | `6379` | Redis port mapping |
| `RABBITMQ_PORT` | `5672` | RabbitMQ AMQP port |
| `RABBITMQ_MGMT_PORT` | `15672` | RabbitMQ management UI port |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASS` | `guest` | RabbitMQ password |
| `POWERLOOM_RPC_NODES` | *required* | Comma-separated RPC node URLs for relayer-py |
| `VPA_SIGNER_ADDRESSES` | *required* | Comma-separated signer addresses (must be in ProtocolState.trustedUpdaters) |
| `VPA_SIGNER_PRIVATE_KEYS` | *required* | Comma-separated signer private keys (hex format, no 0x prefix) |
| `ANCHOR_CHAIN_ID` | `11167` | Anchor chain ID (Powerloom default) |
| `MIN_SIGNER_BALANCE_ETH` | `0` | Minimum signer balance check (0 = disabled for devnet) |

## Usage Examples

### Basic Debugging Mode

Configure `.env`:
```bash
BOOTSTRAP_PEERS=/ip4/1.2.3.4/tcp/4001/p2p/QmPeerID
GOSSIPSUB_TOPIC=/powerloom/snapshot-submissions
```

Then start:
```bash
./start.sh
```

### Validator Mesh Mode (Monitoring Only)

Configure `.env` with validator mesh settings (see Validator Mesh Mode Setup above), then:
```bash
./start.sh
docker-compose logs -f
```

### Full Validator Mesh Mode (With Contract Updates)

Configure `.env` with all settings including contract updates, then:
```bash
./start.sh
docker-compose logs -f
```

## Output Files

### Tally Dumps

Per-epoch JSON files are written to `TALLY_DUMP_DIR`:

```json
{
  "epoch_id": 12345,
  "data_market": "0x...",
  "timestamp": 1234567890,
  "submission_counts": {
    "1": 5,
    "2": 3,
    "3": 7
  },
  "eligible_nodes_count": 10,
  "total_validators": 5,
  "aggregated_projects": {
    "project1": "QmCID1...",
    "project2": "QmCID2..."
  }
}
```

Files are automatically pruned based on retention policies.

## Troubleshooting

### No batches received

- Check bootstrap peers are correct and reachable
- Verify topic names match validator mesh configuration
- Ensure network connectivity to libp2p peers
- Check logs for connection errors

### Events not detected

- Verify `POWERLOOM_RPC_URL` is correct and accessible
- Check `PROTOCOL_STATE_CONTRACT` address is correct
- Ensure contract emits `EpochReleased` events
- Check `EVENT_START_BLOCK` if starting from historical block

### Tally dumps not generated

- Verify `ENABLE_TALLY_DUMPS=true`
- Check `TALLY_DUMP_DIR` is writable
- Ensure aggregation window has closed (check logs)
- Verify batches were received for the epoch

### Contract updates failing

- Check `ENABLE_CONTRACT_UPDATES=true`
- Verify update method configuration (relayer vs direct)
- For relayer: 
  - Check `RELAYER_URL` is correct (use `http://relayer-py:8080` for docker-compose)
  - Verify `RELAYER_AUTH_TOKEN` matches `AUTH_TOKEN` in relayer-py
  - Check relayer-py service is healthy: `docker-compose exec relayer-py curl http://localhost:8080/health`
  - Verify signer addresses are in `ProtocolState.trustedUpdaters` mapping
- For direct: verify `EVM_PRIVATE_KEY` (secp256k1) and RPC access. Note: This is different from `PRIVATE_KEY` (Ed25519) used for libp2p peer identity.
- Check epoch interval matches (updates only every N epochs)
- Verify `POWERLOOM_RPC_URL` is set (required for day fetching)

### Day fetching failures

- Ensure `POWERLOOM_RPC_URL` is set and accessible
- Verify DataMarket contract address is correct
- Check DataMarket ABI file exists at `contract/abi/DataMarket.json`
- Verify RPC node can call view functions on the contract
- Check logs for specific error messages from day fetching

## Development

### Docker (Recommended)

```bash
# Build and start
./start.sh

# View logs
docker-compose logs -f

# Stop
./stop.sh

# Rebuild after code changes
./start.sh
```

### Manual Build (For Development)

```bash
go build -o gossipsub-debugger .
./gossipsub-debugger
```

### Running Tests

```bash
go test ./...
```

## License

[Add license information]

