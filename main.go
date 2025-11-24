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
	"github.com/powerloom/snapshot-sequencer-validator/pkgs/gossipconfig"
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

	// Connect to bootstrap peers
	for _, peerAddr := range bootstrapPeers {
		peerinfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
		if err != nil {
			log.Printf("Failed to parse bootstrap peer address: %v", err)
			continue
		}
		if err := h.Connect(ctx, *peerinfo); err != nil {
			log.Printf("Failed to connect to bootstrap peer %s: %v", peerinfo.ID, err)
		} else {
			log.Printf("Successfully connected to bootstrap peer: %s", peerAddr)
		}
	}

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
			log.Printf("Searching for peers on rendezvous: %s", rendezvous)
			peerChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				log.Printf("Error discovering peers: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}

			for p := range peerChan {
				if p.ID == h.ID() {
					continue
				}
				if h.Network().Connectedness(p.ID) != 2 { // Not connected
					log.Printf("Found peer through discovery: %s", p.ID)
					if err := h.Connect(ctx, p); err != nil {
						log.Printf("Failed to connect to discovered peer %s: %v", p.ID, err)
					} else {
						log.Printf("Connected to discovered peer: %s", p.ID)
					}
				}
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

	// Construct topic from prefix env var
	topicPrefix := os.Getenv("GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX")
	log.Printf("DEBUG: GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX='%s'", topicPrefix)
	if topicPrefix == "" {
		topicPrefix = "/powerloom/snapshot-submissions"
		log.Printf("DEBUG: Using fallback prefix: %s", topicPrefix)
	}
	var topicName string
	if mode == "DISCOVERY" {
		topicName = topicPrefix + "/0"
	} else {
		topicName = topicPrefix + "/all"
	}
	log.Printf("DEBUG: Constructed topic: %s", topicName)

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

	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *listenPort)),
		libp2p.EnableRelay(),
		libp2p.ConnectionManager(connMgr),
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

	// Get standardized gossipsub parameters for snapshot submissions mesh
	gossipParams, peerScoreParams, peerScoreThresholds, paramHash := gossipconfig.ConfigureSnapshotSubmissionsMesh(h.ID())

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
	log.Printf("Initialized gossipsub with standardized snapshot submissions mesh parameters")

	if topicName != "" {
		// Wait a bit for discovery to find peers
		log.Printf("Waiting for peer discovery...")
		time.Sleep(5 * time.Second)

		topic, err := ps.Join(topicName)
		if err != nil {
			log.Fatal(err)
		}

		// In LISTENER or PUBLISHER mode, also join the discovery topic
		var discoveryTopic *pubsub.Topic
		discoveryTopicName := topicPrefix + "/0"
		if (mode == "LISTENER" || mode == "PUBLISHER") && topicName != discoveryTopicName {
			discoveryTopic, err = ps.Join(discoveryTopicName)
			if err != nil {
				log.Printf("Warning: Failed to join discovery topic: %v", err)
			} else {
				log.Printf("Also joined discovery topic: %s", discoveryTopicName)
				// Subscribe to discovery topic
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
						processMessage(msg.Data, "DISCOVERY")
					}
				}()
			}
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
					log.Printf("Received message from %s", msg.GetFrom())
					processMessage(msg.Data, "MAIN")
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
