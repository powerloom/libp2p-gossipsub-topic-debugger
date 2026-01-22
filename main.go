package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	rpchelper "github.com/powerloom/go-rpc-helper"
	"github.com/powerloom/snapshot-sequencer-validator/pkgs/gossipconfig"

	contract "p2p-debugger/contract"
	"p2p-debugger/redis"
)

// P2PSnapshotSubmission represents the data structure for snapshot submissions
// sent over the P2P network by the collector.
type P2PSnapshotSubmission struct {
	EpochID       uint64        `json:"epoch_id"`
	Submissions   []interface{} `json:"submissions"` // Using interface{} for flexibility
	SnapshotterID string        `json:"snapshotter_id"`
	Signature     []byte        `json:"signature"`
}

func setupDHT(ctx context.Context, h host.Host, bootstrapPeers []multiaddr.Multiaddr) (*dht.IpfsDHT, error) {
	// Create DHT in client mode (not a bootstrap node)
	kademliaDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		return nil, err
	}

	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, err
	}

	// Connect to bootstrap peers (filter reserved IPs)
	connectedCount := 0
	filteredCount := 0
	for _, peerAddr := range bootstrapPeers {
		// Filter out reserved IP addresses
		if HasReservedIPAddress(peerAddr) {
			filteredCount++
			continue
		}
		peerinfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
		if err != nil {
			log.Printf("Failed to parse bootstrap peer address: %v", err)
			continue
		}
		// Filter peer addresses
		filteredAddrs, _ := FilterReservedMultiaddrs(peerinfo.Addrs)
		if len(filteredAddrs) == 0 {
			filteredCount++
			continue
		}
		peerinfo.Addrs = filteredAddrs
		if err := h.Connect(ctx, *peerinfo); err != nil {
			log.Printf("Failed to connect to bootstrap peer %s: %v", peerinfo.ID, err)
		} else {
			connectedCount++
		}
	}
	if filteredCount > 0 {
		log.Printf("Filtered %d bootstrap peer(s) with reserved IP addresses", filteredCount)
	}
	log.Printf("Connected to %d/%d bootstrap peer(s)", connectedCount, len(bootstrapPeers)-filteredCount)

	return kademliaDHT, nil
}

func discoverPeers(ctx context.Context, h host.Host, routingDiscovery *routing.RoutingDiscovery, rendezvous string) {
	log.Printf("Starting peer discovery for rendezvous: %s", rendezvous)

	// Advertise on the rendezvous point
	util.Advertise(ctx, routingDiscovery, rendezvous)
	log.Printf("Successfully advertised on rendezvous: %s", rendezvous)

	// Continuously discover peers
	go func() {
		for {
			peerChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				log.Printf("Error discovering peers: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}

			// Track connection attempts for summary logging
			attemptedCount := 0
			connectedCount := 0
			filteredCount := 0

			for p := range peerChan {
				if p.ID == h.ID() {
					continue
				}
				if h.Network().Connectedness(p.ID) != 2 { // Not connected
					// Filter out reserved IP addresses before attempting connection
					filteredAddrs, _ := FilterReservedMultiaddrs(p.Addrs)
					if len(filteredAddrs) == 0 {
						filteredCount++
						continue // Skip peers with only reserved IP addresses
					}
					p.Addrs = filteredAddrs
					attemptedCount++
					if err := h.Connect(ctx, p); err != nil {
						// Only log connection failures (not "no addresses" errors which are common)
						if !strings.Contains(err.Error(), "no addresses") {
							log.Printf("Failed to connect to discovered peer %s: %v", p.ID, err)
						}
					} else {
						connectedCount++
					}
				}
			}

			// Log summary instead of individual peer logs
			if attemptedCount > 0 || filteredCount > 0 {
				log.Printf("Peer discovery summary: attempted=%d, connected=%d, filtered=%d (reserved IPs)", attemptedCount, connectedCount, filteredCount)
			}

			time.Sleep(30 * time.Second) // Wait before next discovery round
		}
	}()
}

// getEnvAsInt gets an environment variable as an integer with a default value
func getEnvAsInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("Invalid value for %s: %s, using default: %d", key, val, defaultValue)
		return defaultValue
	}
	return intVal
}

func main() {
	// Command line flags with env var fallbacks
	privateKeyHex := flag.String("privateKey", os.Getenv("PRIVATE_KEY"), "Hex-encoded private key")

	publishMsg := flag.String("publish", os.Getenv("PUBLISH_MSG"), "Message to publish")
	listPeers := flag.Bool("listPeers", os.Getenv("LIST_PEERS") == "true", "List peers in the topic")
	listenPort := flag.Int("listenPort", 8001, "Port to listen on (default: 8001)")
	publicIP := flag.String("publicIP", os.Getenv("PUBLIC_IP"), "Public IP to advertise")

	// Rendezvous: Use RENDEZVOUS_POINT env var with fallback
	rendezvousDefault := os.Getenv("RENDEZVOUS_POINT")
	if rendezvousDefault == "" {
		rendezvousDefault = "powerloom-snapshot-sequencer-network"
	}
	rendezvousString := flag.String("rendezvous", rendezvousDefault, "Rendezvous string for peer discovery")
	flag.Parse()

	// Override with env vars if set (for backward compatibility)
	if envPort := os.Getenv("LISTEN_PORT"); envPort != "" {
		if port, err := fmt.Sscanf(envPort, "%d", listenPort); err == nil && port == 1 {
			// Port successfully parsed
		}
	}

	// Check MODE env var
	mode := os.Getenv("MODE")
	publishInterval := 30 // default 30 seconds
	if envInterval := os.Getenv("PUBLISH_INTERVAL"); envInterval != "" {
		if interval, err := fmt.Sscanf(envInterval, "%d", &publishInterval); err == nil && interval == 1 {
			// Interval successfully parsed
		}
	}

	// Check if validator mesh mode is enabled
	validatorMeshMode := os.Getenv("VALIDATOR_MESH_MODE") == "true"

	var topicPrefix string
	var topicName string
	var discoveryTopicName string
	var presenceTopicName string

	if validatorMeshMode {
		// Validator mesh topics (devnet defaults)
		topicPrefix = os.Getenv("GOSSIPSUB_FINALIZED_BATCH_PREFIX")
		if topicPrefix == "" {
			topicPrefix = "/powerloom/dsv-devnet-alpha/finalized-batches"
		}
		discoveryTopicName = topicPrefix + "/0"
		topicName = topicPrefix + "/all"

		presenceTopicName = os.Getenv("GOSSIPSUB_VALIDATOR_PRESENCE_TOPIC")
		if presenceTopicName == "" {
			presenceTopicName = "/powerloom/dsv-devnet-alpha/validator/presence"
		}

		log.Printf("Running in VALIDATOR MESH mode")
		log.Printf("Batch discovery topic: %s", discoveryTopicName)
		log.Printf("Batch topic: %s", topicName)
		log.Printf("Presence topic: %s", presenceTopicName)
	} else {
		// Snapshotter mesh topics (legacy)
		topicPrefix = os.Getenv("GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX")
		log.Printf("DEBUG: GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX='%s'", topicPrefix)
		if topicPrefix == "" {
			topicPrefix = "/powerloom/snapshot-submissions"
			log.Printf("DEBUG: Using fallback prefix: %s", topicPrefix)
		}
		if mode == "DISCOVERY" {
			topicName = topicPrefix + "/0"
		} else {
			topicName = topicPrefix + "/all"
		}
		discoveryTopicName = topicPrefix + "/0"
		log.Printf("DEBUG: Constructed topic: %s", topicName)
	}

	// Auto-configure based on MODE
	switch mode {
	case "PUBLISHER":
		if *publishMsg == "" {
			*publishMsg = "auto-test-message"
		}
		*listPeers = true // Also show peers in publisher mode
		log.Printf("Running in PUBLISHER mode: publish to=%s, interval=%ds", topicName, publishInterval)
		log.Printf("Note: Will also monitor discovery topic %s/0", topicPrefix)
	case "LISTENER":
		*listPeers = true
		*publishMsg = "" // Don't publish in listener mode
		log.Printf("Running in LISTENER mode: primary topic=%s", topicName)
		log.Printf("Note: Will also monitor discovery topic %s/0", topicPrefix)
	case "DISCOVERY":
		*listPeers = true
		log.Printf("Running in DISCOVERY mode: topic=%s", topicName)
	default:
		// Use flags as provided
		log.Printf("Running with custom configuration")
	}

	// Initialize logger
	initLogger()

	ctx := context.Background()

	// Initialize Redis client for submission count persistence
	redisClient, err := redis.NewRedisClient()
	if err != nil {
		log.Printf("⚠️ Failed to initialize Redis client: %v. Submission counts will only be stored in memory.", err)
		log.Printf("⚠️ This means counts will be lost on restart. For production, ensure Redis is available.")
	} else {
		redis.RedisClient = redisClient
		log.Printf("✅ Redis client initialized successfully")
		defer redisClient.Close()
	}

	// Configure connection manager for testing/debugging
	connLowWater := getEnvAsInt("CONN_MANAGER_LOW_WATER", 20)
	connHighWater := getEnvAsInt("CONN_MANAGER_HIGH_WATER", 100)
	connMgr, err := connmgr.NewConnManager(
		connLowWater,
		connHighWater,
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		log.Fatalf("Failed to create connection manager: %v", err)
	}
	log.Printf("Connection manager configured: LowWater=%d, HighWater=%d (debugger mode)", connLowWater, connHighWater)

	var privKey crypto.PrivKey
	if *privateKeyHex == "" {
		privKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		keyBytes, err := hex.DecodeString(*privateKeyHex)
		if err != nil {
			log.Fatal(err)
		}
		privKey, err = crypto.UnmarshalEd25519PrivateKey(keyBytes)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create RFC1918 connection gater to block reserved IP addresses
	rfc1918Gater := &RFC1918ConnectionGater{}

	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *listenPort)),
		libp2p.EnableRelay(),
		libp2p.ConnectionManager(connMgr),
		libp2p.ConnectionGater(rfc1918Gater),
	}

	if *publicIP != "" {
		publicAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", *publicIP, *listenPort))
		if err != nil {
			log.Printf("Failed to create public multiaddr: %v", err)
		} else {
			opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return append(addrs, publicAddr)
			}))
		}
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	log.Printf("Host created with ID: %s", h.ID())

	// Parse bootstrap peers (support multiple comma-separated addresses)
	bootstrapPeersStr := os.Getenv("BOOTSTRAP_PEERS")
	if bootstrapPeersStr == "" {
		// Fallback to old env var names for backward compatibility
		bootstrapPeersStr = os.Getenv("BOOTSTRAP_ADDRS")
		if bootstrapPeersStr == "" {
			bootstrapPeersStr = os.Getenv("BOOTSTRAP_ADDR")
		}
	}
	if bootstrapPeersStr == "" {
		log.Fatal("BOOTSTRAP_PEERS environment variable is required")
	}

	// Split by comma and parse each address
	bootstrapAddrs := []multiaddr.Multiaddr{}
	for _, addrStr := range strings.Split(bootstrapPeersStr, ",") {
		addrStr = strings.TrimSpace(addrStr)
		if addrStr == "" {
			continue
		}
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			log.Printf("Warning: Failed to parse bootstrap address '%s': %v", addrStr, err)
			continue
		}
		bootstrapAddrs = append(bootstrapAddrs, addr)
	}

	if len(bootstrapAddrs) == 0 {
		log.Fatal("No valid bootstrap addresses found")
	}

	log.Printf("Connecting to %d bootstrap peer(s)", len(bootstrapAddrs))
	// Setup DHT
	kademliaDHT, err := setupDHT(ctx, h, bootstrapAddrs)
	if err != nil {
		log.Fatal(err)
	}

	// Create routing discovery
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)

	// Start peer discovery on the rendezvous point
	discoverPeers(ctx, h, routingDiscovery, *rendezvousString)

	// If a topic is specified, also discover on that topic
	if topicName != "" {
		discoverPeers(ctx, h, routingDiscovery, topicName)
	}
	if discoveryTopicName != "" && discoveryTopicName != topicName {
		discoverPeers(ctx, h, routingDiscovery, discoveryTopicName)
	}
	if presenceTopicName != "" {
		discoverPeers(ctx, h, routingDiscovery, presenceTopicName)
	}

	// Initialize components for validator mesh mode
	var batchProcessor *BatchProcessor
	var submissionCounter *SubmissionCounter
	var contractClient *contract.Client
	var lastFetchedDay map[string]string // Track last fetched day per data market for comparison
	var contractUpdater *contract.Updater
	var quotaCache *QuotaCache
	var windowManager *WindowManager
	var eventMonitor *EventMonitor
	var tallyDumper *TallyDumper
	var dayTransitionManager *DayTransitionManager

	if validatorMeshMode {
		// Get configured data market address (REQUIRED)
		// Note: Level 2 batches don't contain dataMarket info, so we use a single configured value
		configuredDataMarket := os.Getenv("DATA_MARKET_ADDRESS")
		if configuredDataMarket == "" {
			log.Fatal("DATA_MARKET_ADDRESS environment variable is required for validator mesh mode")
		}
		log.Printf("Using configured data market address: %s", configuredDataMarket)

		// Initialize window manager
		windowManager = NewWindowManager(ctx)

		// Initialize day transition manager
		dayTransitionManager = NewDayTransitionManager(ctx)

		// Initialize tally dumper
		tallyDumper = NewTallyDumper()
		if err := tallyDumper.Initialize(ctx); err != nil {
			log.Fatalf("Failed to initialize tally dumper: %v", err)
		}

		// Initialize batch processor with window manager
		batchProcessor = NewBatchProcessor(ctx, windowManager)

		// Set data market extractor to return configured data market (for window lookups)
		// Windows are stored using configured/new addresses, not legacy event monitor addresses
		// (Level 2 batches don't contain dataMarket info atm)
		batchProcessor.SetDataMarketExtractor(func(batch *FinalizedBatch) string {
			return configuredDataMarket
		})

		// Initialize submission counter (with Redis persistence)
		submissionCounter = NewSubmissionCounter(ctx)

		// Initialize contract client (may be disabled)
		contractClient, err = contract.NewClient()
		if err != nil {
			log.Fatalf("Failed to initialize contract client: %v", err)
		}
		defer contractClient.Close()

		// Initialize contract updater
		contractUpdater = contract.NewUpdater(contractClient)

		// Initialize quota cache (for dailySnapshotQuota)
		quotaCache = NewQuotaCache(ctx, contractClient)
		// Load quota from Redis on startup
		if configuredDataMarket != "" {
			quotaCache.LoadFromRedis([]string{configuredDataMarket})
		}

		// Initialize last fetched day tracker
		lastFetchedDay = make(map[string]string)

		// Set window close callback - triggers aggregation and tally dump
		// Note: dataMarket parameter comes from EpochReleased event, but we use configured value
		windowManager.SetWindowCloseCallback(func(epochID uint64, dataMarket string) error {
			// Use configured data market (Level 2 batches don't contain dataMarket info atm)
			dataMarket = configuredDataMarket
			log.Printf("🔒 Window closed for epoch %d, dataMarket %s - finalizing tally", epochID, dataMarket)

			// Create a new context with timeout for contract calls (window close callback runs in goroutine)
			callCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Get aggregation state before aggregating
			agg := batchProcessor.GetEpochAggregation(epochID)
			if agg == nil {
				log.Printf("⚠️  No aggregation found for epoch %d", epochID)
				return fmt.Errorf("no aggregation found for epoch %d", epochID)
			}

			log.Printf("📊 Aggregating epoch %d: %d batches received from %d validators",
				epochID, agg.ReceivedBatches, agg.TotalValidators)

			// Aggregate batches for this epoch (aggregate once per epoch, not per data market)
			if err := batchProcessor.aggregateEpoch(epochID, dataMarket); err != nil {
				return fmt.Errorf("failed to aggregate epoch: %w", err)
			}

			// Get aggregated batch after aggregation
			agg = batchProcessor.GetEpochAggregation(epochID)
			if agg == nil || agg.AggregatedBatch == nil {
				log.Printf("⚠️  No aggregated batch found for epoch %d after aggregation", epochID)
				return nil
			}

			// Extract submission counts from ALL Level 1 batches using winning CIDs from aggregated batch
			// A slot is eligible only if its submission CID matches the winning CID for that project
			slotCounts, err := ExtractSubmissionCountsFromBatches(agg.Batches, agg.AggregatedBatch, dataMarket)
			if err != nil {
				return fmt.Errorf("failed to extract submission counts: %w", err)
			}

			log.Printf("📈 Extracted %d unique slot IDs for epoch %d", len(slotCounts), epochID)

			// Fetch current day once for both count tracking and day transition checking
			currentDay := ""
			if contractClient != nil {
				day, err := contractClient.FetchCurrentDay(callCtx, common.HexToAddress(dataMarket))
				if err != nil {
					log.Printf("⚠️  Could not fetch current day for epoch %d: %v", epochID, err)
				} else {
					currentDay = day.String()
					// Compare with last fetched day to detect changes
					lastDay, exists := lastFetchedDay[dataMarket]
					if !exists {
						log.Printf("📅 First day fetch for data market %s: day %s (epoch %d)", dataMarket, currentDay, epochID)
						lastFetchedDay[dataMarket] = currentDay
					} else if lastDay != currentDay {
						log.Printf("📅 Day changed for data market %s: %s -> %s (epoch %d)", dataMarket, lastDay, currentDay, epochID)
						lastFetchedDay[dataMarket] = currentDay
					} else {
						log.Printf("📅 Day unchanged for data market %s: day %s (epoch %d)", dataMarket, currentDay, epochID)
					}
				}
			}

			// Get daily snapshot quota for eligibility check
			dailySnapshotQuota := 0
			if quotaCache != nil {
				quota, err := quotaCache.GetQuotaWithContext(callCtx, dataMarket)
				if err != nil {
					log.Printf("⚠️ Failed to get dailySnapshotQuota for eligibility check: %v. Using count > 0 as fallback.", err)
				} else {
					dailySnapshotQuota = int(quota.Int64())
				}
			}

			// Update submission counter with day tracking
			if currentDay != "" {
				if err := submissionCounter.UpdateEligibleCountsForDay(epochID, dataMarket, currentDay, slotCounts, dailySnapshotQuota); err != nil {
					return fmt.Errorf("failed to update eligible counts: %w", err)
				}
			} else {
				// Fallback: update without day tracking
				log.Printf("⚠️  Updating counts without day tracking (day fetch failed)")
				if err := submissionCounter.UpdateEligibleCounts(epochID, dataMarket, slotCounts); err != nil {
					return fmt.Errorf("failed to update eligible counts: %w", err)
				}
			}

			// Extract validator batch CIDs
			validatorBatchCIDs := make(map[string]string)
			for _, batch := range agg.Batches {
				if batch.SequencerId != "" && batch.BatchIPFSCID != "" {
					validatorBatchCIDs[batch.SequencerId] = batch.BatchIPFSCID
				}
			}

			// Calculate eligible nodes count for THIS epoch only (slots with count > 0)
			eligibleNodesCount := 0
			for _, count := range slotCounts {
				if count > 0 {
					eligibleNodesCount++
				}
			}

			// Generate tally dump for the specific data market that triggered the window close
			if err := tallyDumper.Dump(epochID, dataMarket, slotCounts, eligibleNodesCount, agg.TotalValidators, agg.AggregatedProjects, validatorBatchCIDs); err != nil {
				log.Printf("❌ Error generating tally dump: %v", err)
			}

			// Check for day transition (using the same currentDay fetched above)
			if currentDay != "" {
				dayTransitionManager.CheckDayTransition(dataMarket, currentDay, epochID)
			} else {
				log.Printf("⚠️  Skipping day transition check for epoch %d (day fetch failed)", epochID)
			}

			// Update quota cache for this epoch (queries contract periodically)
			if quotaCache != nil {
				if err := quotaCache.UpdateQuotaForEpochWithContext(callCtx, dataMarket, epochID); err != nil {
					log.Printf("⚠️ Failed to update quota cache for epoch %d: %v", epochID, err)
				}
			}

			// Check if contract updater is initialized
			if contractUpdater == nil {
				log.Printf("⚠️  Contract updater not initialized (ENABLE_CONTRACT_UPDATES may be false) - skipping contract updates for epoch %d", epochID)
				return nil
			}

			// Check if this is a buffer epoch (final update for previous day)
			if marker, isBufferEpoch := dayTransitionManager.IsBufferEpoch(dataMarket, epochID); isBufferEpoch {
				log.Printf("🎯 Buffer epoch reached for data market %s: epoch %d (previous day: %s)",
					dataMarket, epochID, marker.LastKnownDay)

				// Get daily snapshot quota from cache (in-memory or Redis)
				dailySnapshotQuota := 0
				if quotaCache != nil {
					quota, err := quotaCache.GetQuotaWithContext(callCtx, dataMarket)
					if err != nil {
						log.Printf("⚠️ Failed to get dailySnapshotQuota for data market %s: %v. Using count > 0 as fallback.", dataMarket, err)
					} else {
						dailySnapshotQuota = int(quota.Int64())
						log.Printf("📊 Daily snapshot quota for data market %s: %d", dataMarket, dailySnapshotQuota)
					}
				}

				// Send final update for previous day with eligibleNodesCount
				// Get submission counts for the previous day (from Redis)
				prevDaySlotCounts := submissionCounter.GetCountsForDay(dataMarket, marker.LastKnownDay)
				// Get eligible nodes count (slots with count >= dailySnapshotQuota)
				prevDayEligibleNodes := submissionCounter.GetEligibleNodesCountForDay(dataMarket, marker.LastKnownDay, dailySnapshotQuota)

				if err := contractUpdater.UpdateFinalRewards(callCtx, epochID, dataMarket, marker.LastKnownDay, prevDaySlotCounts, prevDayEligibleNodes); err != nil {
					log.Printf("❌ Error sending final rewards update for data market %s, day %s: %v", dataMarket, marker.LastKnownDay, err)
				} else {
					log.Printf("✅ Successfully sent final rewards update for data market %s, day %s (eligibleNodes=%d)",
						dataMarket, marker.LastKnownDay, prevDayEligibleNodes)
					// Remove marker after successful update
					dayTransitionManager.RemoveMarker(dataMarket, marker.CurrentEpoch)
					// Optionally reset counts for the previous day after final update
					submissionCounter.ResetCountsForDay(dataMarket, marker.LastKnownDay)
				}
			} else {
				// Periodic update - send accumulated counts for the current day (not per-epoch counts)
				if currentDay != "" {
					// Get accumulated counts for the current day from Redis
					accumulatedDayCounts := submissionCounter.GetCountsForDay(dataMarket, currentDay)
					if len(accumulatedDayCounts) > 0 {
						if err := contractUpdater.UpdateSubmissionCounts(callCtx, epochID, dataMarket, accumulatedDayCounts, 0); err != nil {
							log.Printf("❌ Error updating contract for data market %s: %v", dataMarket, err)
						}
						// Note: UpdateSubmissionCounts logs internally when skipping or successfully sending
					} else {
						log.Printf("⚠️  No accumulated counts found for day %s, skipping periodic update", currentDay)
					}
				} else {
					log.Printf("⚠️  Current day not available, skipping periodic update for epoch %d", epochID)
				}
			}

			return nil
		})

		// Initialize event monitor if RPC nodes are provided
		rpcNodesStr := os.Getenv("POWERLOOM_RPC_NODES")
		if rpcNodesStr == "" {
			log.Printf("Event monitoring disabled (POWERLOOM_RPC_NODES not set)")
		} else {
			nodes := strings.Split(rpcNodesStr, ",")
			if len(nodes) == 0 {
				log.Printf("Event monitoring disabled (POWERLOOM_RPC_NODES is empty)")
			} else {
				// Use PROTOCOL_STATE_CONTRACT and DATA_MARKET_ADDRESS for event monitoring
				eventProtocolContract := os.Getenv("PROTOCOL_STATE_CONTRACT")
				eventDataMarket := configuredDataMarket

				if eventProtocolContract == "" {
					log.Printf("Event monitoring disabled (PROTOCOL_STATE_CONTRACT not set)")
				} else {
					// Build RPC config for event monitoring
					rpcConfig := &rpchelper.RPCConfig{
						Nodes: func() []rpchelper.NodeConfig {
							var nodeConfigs []rpchelper.NodeConfig
							for _, url := range nodes {
								url = strings.TrimSpace(url)
								if url != "" {
									nodeConfigs = append(nodeConfigs, rpchelper.NodeConfig{URL: url})
								}
							}
							return nodeConfigs
						}(),
						MaxRetries:     5,
						RetryDelay:     200 * time.Millisecond,
						MaxRetryDelay:  5 * time.Second,
						RequestTimeout: 30 * time.Second,
					}

					eventRpcHelper := rpchelper.NewRPCHelper(rpcConfig)
					initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					if err := eventRpcHelper.Initialize(initCtx); err != nil {
						cancel()
						log.Fatalf("Failed to initialize RPC helper for event monitoring: %v", err)
					}
					cancel()

					// Filter events by data market
					dataMarketsFilter := []string{eventDataMarket}

					log.Printf("Event monitoring: protocolState=%s, dataMarket=%s", eventProtocolContract, eventDataMarket)

					eventMonitor, err = NewEventMonitor(ctx, eventRpcHelper, eventProtocolContract, dataMarketsFilter)
					if err != nil {
						log.Fatalf("Failed to initialize event monitor: %v", err)
					}
					defer eventMonitor.Close()

					eventMonitor.SetEventCallback(func(event *EpochReleasedEvent) error {
						// Always call window manager first (creates windows for batch processing)
						if err := windowManager.OnEpochReleased(event); err != nil {
							return err
						}

						// CRITICAL: Check day transitions and buffer epochs on EVERY epoch,
						// even if no batches arrive. This ensures final rewards are processed
						// even when all nodes are down.
						if contractClient != nil && dayTransitionManager != nil && contractUpdater != nil && submissionCounter != nil {
							epochID := event.EpochID.Uint64()
							dataMarket := configuredDataMarket

							// Create context for contract calls
							callCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()

							// Fetch current day for day transition checking
							day, err := contractClient.FetchCurrentDay(callCtx, common.HexToAddress(dataMarket))
							if err != nil {
								log.Printf("⚠️  Could not fetch current day for epoch %d (EpochReleased): %v", epochID, err)
							} else {
								currentDay := day.String()
								// Update last fetched day tracker
								lastDay, exists := lastFetchedDay[dataMarket]
								if !exists {
									lastFetchedDay[dataMarket] = currentDay
									log.Printf("📅 [EpochReleased] First day fetch for data market %s: day %s (epoch %d)", dataMarket, currentDay, epochID)
								} else if lastDay != currentDay {
									lastFetchedDay[dataMarket] = currentDay
									log.Printf("📅 [EpochReleased] Day changed for data market %s: %s -> %s (epoch %d)", dataMarket, lastDay, currentDay, epochID)
								}

								// Check for day transition (independent of batches)
								dayTransitionManager.CheckDayTransition(dataMarket, currentDay, epochID)

								// Check if this is a buffer epoch (independent of batches)
								if marker, isBufferEpoch := dayTransitionManager.IsBufferEpoch(dataMarket, epochID); isBufferEpoch {
									log.Printf("🎯 [EpochReleased] Buffer epoch reached for data market %s: epoch %d (previous day: %s)",
										dataMarket, epochID, marker.LastKnownDay)

									// Get daily snapshot quota from cache
									dailySnapshotQuota := 0
									if quotaCache != nil {
										quota, err := quotaCache.GetQuotaWithContext(callCtx, dataMarket)
										if err != nil {
											log.Printf("⚠️ Failed to get dailySnapshotQuota for data market %s: %v. Using count > 0 as fallback.", dataMarket, err)
										} else {
											dailySnapshotQuota = int(quota.Int64())
											log.Printf("📊 [EpochReleased] Daily snapshot quota for data market %s: %d", dataMarket, dailySnapshotQuota)
										}
									}

									// Get submission counts for the previous day (from Redis)
									prevDaySlotCounts := submissionCounter.GetCountsForDay(dataMarket, marker.LastKnownDay)
									// Get eligible nodes count (slots with count >= dailySnapshotQuota)
									prevDayEligibleNodes := submissionCounter.GetEligibleNodesCountForDay(dataMarket, marker.LastKnownDay, dailySnapshotQuota)

									log.Printf("📊 [EpochReleased] Processing final rewards for day %s: %d slots, %d eligible nodes",
										marker.LastKnownDay, len(prevDaySlotCounts), prevDayEligibleNodes)

									if err := contractUpdater.UpdateFinalRewards(callCtx, epochID, dataMarket, marker.LastKnownDay, prevDaySlotCounts, prevDayEligibleNodes); err != nil {
										log.Printf("❌ [EpochReleased] Error sending final rewards update for data market %s, day %s: %v", dataMarket, marker.LastKnownDay, err)
									} else {
										log.Printf("✅ [EpochReleased] Successfully sent final rewards update for data market %s, day %s (eligibleNodes=%d)",
											dataMarket, marker.LastKnownDay, prevDayEligibleNodes)
										// Remove marker after successful update
										dayTransitionManager.RemoveMarker(dataMarket, marker.CurrentEpoch)
										// Optionally reset counts for the previous day after final update
										submissionCounter.ResetCountsForDay(dataMarket, marker.LastKnownDay)
									}
								}
							}
						}

						return nil
					})

					// Start event monitoring
					if err := eventMonitor.Start(); err != nil {
						log.Fatalf("Failed to start event monitor: %v", err)
					}

					log.Printf("Started event monitoring for protocol contract %s", eventProtocolContract)
				}
			}
		}

		log.Printf("Initialized validator mesh components")
	}

	// Get gossipsub parameters based on mode
	var gossipParams *pubsub.GossipSubParams
	var peerScoreParams *pubsub.PeerScoreParams
	var peerScoreThresholds *pubsub.PeerScoreThresholds
	var paramHash string

	if validatorMeshMode {
		// Use validator mesh configuration from shared package
		validatorTopics := []string{discoveryTopicName, topicName}
		if presenceTopicName != "" {
			validatorTopics = append(validatorTopics, presenceTopicName)
		}
		// This utility only listens to finalized batches, not consensus votes/proposals.
		// Passing empty strings for consensus topics since ConfigureValidatorVotesMesh
		// requires them but we don't join those topics.
		gossipParams, peerScoreParams, peerScoreThresholds = gossipconfig.ConfigureValidatorVotesMesh(h.ID(), validatorTopics, "", "")
		paramHash = gossipconfig.GenerateParamHash(gossipParams)
		log.Printf("Using validator mesh gossipsub configuration")
	} else {
		// Use snapshot submissions mesh configuration
		discoveryTopic, submissionsTopic := discoveryTopicName, topicName
		gossipParams, peerScoreParams, peerScoreThresholds, paramHash = gossipconfig.ConfigureSnapshotSubmissionsMesh(h.ID(), discoveryTopic, submissionsTopic)
		log.Printf("Using snapshot submissions mesh gossipsub configuration")
	}

	// Create gossipsub with standardized parameters
	ps, err := pubsub.NewGossipSub(
		ctx,
		h,
		pubsub.WithGossipSubParams(*gossipParams),
		pubsub.WithPeerScore(peerScoreParams, peerScoreThresholds),
		pubsub.WithDiscovery(routingDiscovery),
		pubsub.WithFloodPublish(true),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("🔑 Gossipsub parameter hash: %s (p2p-debugger %s mode)", paramHash, mode)
	if validatorMeshMode {
		log.Printf("Initialized gossipsub with validator mesh parameters")
	} else {
		log.Printf("Initialized gossipsub with standardized snapshot submissions mesh parameters")
	}

	if topicName != "" {
		// Wait a bit for discovery to find peers
		log.Printf("Waiting for peer discovery...")
		time.Sleep(5 * time.Second)

		topic, err := ps.Join(topicName)
		if err != nil {
			log.Fatal(err)
		}

		// Join discovery topic
		var discoveryTopic *pubsub.Topic
		if discoveryTopicName != "" && topicName != discoveryTopicName {
			discoveryTopic, err = ps.Join(discoveryTopicName)
			if err != nil {
				log.Printf("Warning: Failed to join discovery topic: %v", err)
			} else {
				log.Printf("Also joined discovery topic: %s", discoveryTopicName)
			}
		}

		// Join presence topic (validator mesh only)
		if validatorMeshMode && presenceTopicName != "" {
			_, err = ps.Join(presenceTopicName)
			if err != nil {
				log.Printf("Warning: Failed to join presence topic: %v", err)
			} else {
				log.Printf("Also joined presence topic: %s", presenceTopicName)
			}
		}

		// Subscribe to discovery topic
		if discoveryTopic != nil {
			go func() {
				sub, err := discoveryTopic.Subscribe()
				if err != nil {
					log.Printf("Failed to subscribe to discovery topic: %v", err)
					return
				}
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						log.Printf("Error getting discovery topic message: %v", err)
						continue
					}
					// Skip our own messages
					if msg.GetFrom() == h.ID() {
						continue
					}
					log.Printf("[DISCOVERY] Received message from %s", msg.GetFrom())
					if validatorMeshMode {
						processValidatorMessage(msg.Data, batchProcessor, windowManager, "DISCOVERY")
					} else {
						processMessage(msg.Data, "DISCOVERY")
					}
				}
			}()
		}

		if *listPeers {
			go func() {
				for {
					time.Sleep(5 * time.Second)
					// List peers in the topic mesh
					peers := ps.ListPeers(topicName)
					log.Printf("Peers in topic %s: %v (count: %d)", topicName, peers, len(peers))

					// In LISTENER or PUBLISHER mode, also show discovery topic peers
					if (mode == "LISTENER" || mode == "PUBLISHER") && topicName != discoveryTopicName {
						discoveryPeers := ps.ListPeers(discoveryTopicName)
						log.Printf("Peers in discovery topic (joining room): %v (count: %d)", discoveryPeers, len(discoveryPeers))
					}

					// Also show total connected peers
					connectedPeers := h.Network().Peers()
					log.Printf("Total connected peers: %d", len(connectedPeers))
				}
			}()
		}

		if *publishMsg != "" {
			// Wait for mesh to form
			log.Printf("Waiting 10 seconds for mesh to form...")
			time.Sleep(10 * time.Second)

			// Publish messages at configured interval
			go func(interval int) {
				messageCount := 0
				log.Printf("Starting publisher loop with %d second interval", interval)
				for {
					messageCount++
					// Create a test message with incrementing data
					testMessage := fmt.Sprintf(`{
						"epochId": %d,
						"projectId": "test_project_%d",
						"snapshotCid": "QmTest%d%d",
						"timestamp": %d,
						"message": "%s",
						"testMessage": true,
						"messageNumber": %d
					}`, messageCount, messageCount, messageCount, time.Now().Unix(), time.Now().Unix(), *publishMsg, messageCount)

					if err := topic.Publish(ctx, []byte(testMessage)); err != nil {
						log.Printf("Failed to publish message #%d: %v", messageCount, err)
					} else {
						log.Printf("Published message #%d to topic %s", messageCount, topicName)
						log.Printf("Message content: %s", testMessage)
					}

					// Every 3rd message, also publish to discovery topic if available
					if messageCount%3 == 0 && discoveryTopic != nil && mode == "PUBLISHER" {
						discoveryMessage := fmt.Sprintf(`{
							"type": "presence",
							"peerId": "%s",
							"timestamp": %d,
							"message": "Publisher active in joining room"
						}`, h.ID(), time.Now().Unix())

						if err := discoveryTopic.Publish(ctx, []byte(discoveryMessage)); err != nil {
							log.Printf("Failed to publish presence to discovery topic: %v", err)
						} else {
							log.Printf("[DISCOVERY] Published presence message to epoch 0")
						}
					}

					// Also show current peer count
					peers := ps.ListPeers(topicName)
					log.Printf("Current peers in topic mesh: %d", len(peers))

					// Wait configured interval before next publish
					time.Sleep(time.Duration(interval) * time.Second)
				}
			}(publishInterval)
		} else {
			sub, err := topic.Subscribe()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Subscribed to topic: %s", topicName)

			// Handle incoming messages
			go func() {
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						log.Printf("Error getting next message: %v", err)
						continue
					}
					// Skip our own messages
					if msg.GetFrom() == h.ID() {
						continue
					}
					if validatorMeshMode {
						processValidatorMessage(msg.Data, batchProcessor, windowManager, msg.GetFrom().String())
					} else {
						processMessage(msg.Data, "MAIN")
					}
				}
			}()
		}
	}

	select {}
}

func processMessage(data []byte, source string) {
	// First try to unmarshal as P2PSnapshotSubmission
	var p2pSubmission P2PSnapshotSubmission
	err := json.Unmarshal(data, &p2pSubmission)
	if err == nil && p2pSubmission.SnapshotterID != "" {
		// Successfully unmarshalled as P2P submission
		log.Printf("[%s] P2P Submission from snapshotter %s:", source, p2pSubmission.SnapshotterID)
		log.Printf("  Epoch ID: %d", p2pSubmission.EpochID)
		log.Printf("  Number of submissions: %d", len(p2pSubmission.Submissions))
		if len(p2pSubmission.Submissions) > 0 {
			// Try to pretty print first submission
			if submissionBytes, err := json.MarshalIndent(p2pSubmission.Submissions[0], "  ", "  "); err == nil {
				log.Printf("  First submission: %s", string(submissionBytes))
			}
		}
	} else {
		// Try to parse as regular JSON
		var genericMsg map[string]interface{}
		if err := json.Unmarshal(data, &genericMsg); err == nil {
			if prettyJSON, err := json.MarshalIndent(genericMsg, "  ", "  "); err == nil {
				log.Printf("[%s] JSON message:\n%s", source, string(prettyJSON))
			} else {
				log.Printf("[%s] Message: %s", source, string(data))
			}
		} else {
			// Not JSON, print as raw string
			log.Printf("[%s] Raw message: %s", source, string(data))
		}
	}
}

// processValidatorMessage processes validator batch messages
func processValidatorMessage(data []byte, batchProcessor *BatchProcessor, windowManager *WindowManager, peerID string) {
	// Try to parse as FinalizedBatch (the actual JSON structure)
	batch, err := ParseValidatorBatchMessage(data)
	if err == nil && batch.SequencerId != "" && batch.EpochId > 0 {
		// Use configured data market (Level 2 batches don't contain dataMarket info atm)
		configuredDataMarket := os.Getenv("DATA_MARKET_ADDRESS")
		if configuredDataMarket == "" {
			log.Printf("⚠️  Warning: DATA_MARKET_ADDRESS not set, skipping batch validation")
			// Still process the batch, but window checks will be skipped
		}

		// Check if we can accept this batch (must be past Level 1 delay)
		if windowManager != nil && configuredDataMarket != "" {
			if !windowManager.CanAcceptBatch(batch.EpochId, configuredDataMarket) {
				log.Printf("⏸️  Skipping batch - Level 1 finalization delay not yet completed (epoch %d, validator %s)",
					batch.EpochId, batch.SequencerId)
				return
			}
		}

		// Process the batch
		if batchProcessor != nil {
			if err := batchProcessor.ProcessValidatorBatch(batch); err != nil {
				log.Printf("❌ Error processing validator batch: %v", err)
			}
		}
		return
	}

	// Try to parse as presence message
	var presenceMsg map[string]interface{}
	if err := json.Unmarshal(data, &presenceMsg); err == nil {
		if msgType, ok := presenceMsg["type"].(string); ok && msgType == "validator_presence" {
			if peerIDVal, ok := presenceMsg["peer_id"].(string); ok {
				log.Printf("👋 Validator presence: %s", peerIDVal)
			}
		} else {
			// Unknown message type - log briefly
			if epochID, ok := presenceMsg["EpochId"].(float64); ok {
				log.Printf("📨 Unknown message type for epoch %.0f from %s", epochID, peerID)
			} else {
				log.Printf("📨 Unknown message from %s", peerID)
			}
		}
	} else {
		// Not JSON or parse error
		log.Printf("⚠️  Failed to parse message from %s: %v", peerID, err)
	}
}
