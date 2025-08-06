package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"time"

	"fmt"
	"os"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func main() {
	privateKeyHex := flag.String("privateKey", "", "Hex-encoded private key")
	topicName := flag.String("topic", "", "Gossipsub topic to subscribe/publish to")
	publishMsg := flag.String("publish", "", "Message to publish")
	listPeers := flag.Bool("listPeers", false, "List peers in the topic")
	listenPort := flag.Int("listenPort", 8001, "Port to listen on (default: 8001)")
	publicIP := flag.String("publicIP", "", "Public IP to advertise")
	flag.Parse()

	ctx := context.Background()

	var privKey crypto.PrivKey
	var err error
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

	bootstrapPeer := os.Getenv("BOOTSTRAP_ADDR")
	if bootstrapPeer == "" {
		log.Fatal("BOOTSTRAP_ADDR environment variable is required")
	}

	addr, err := multiaddr.NewMultiaddr(bootstrapPeer)
	if err != nil {
		log.Fatal(err)
	}
	peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Connect(ctx, *peerInfo); err != nil {
		log.Printf("Failed to connect to bootstrap peer: %v", err)
	} else {
		log.Printf("Successfully connected to bootstrap peer: %s", bootstrapPeer)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal(err)
	}

	if *topicName != "" {
		topic, err := ps.Join(*topicName)
		if err != nil {
			log.Fatal(err)
		}

		if *listPeers {
			go func() {
				for {
					time.Sleep(5 * time.Second)
					// Use the DHT to find peers in the topic
					peers := ps.ListPeers(*topicName)
					log.Printf("Peers in topic %s: %v", *topicName, peers)
				}
			}()
		}

		if *publishMsg != "" {
			if err := topic.Publish(ctx, []byte(*publishMsg)); err != nil {
				log.Printf("Failed to publish message: %v", err)
			}
			log.Printf("Published message to topic %s: %s", *topicName, *publishMsg)
		} else {
			sub, err := topic.Subscribe()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Subscribed to topic: %s", *topicName)
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					log.Printf("Error getting next message: %v", err)
					continue
				}
				log.Printf("Received message from %s: %s", msg.GetFrom(), string(msg.Data))
			}
		}
	}

	select {}
}
