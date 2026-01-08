package main

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/grpc"
	"golang.org/x/time/rate"
)

var (
	// Pool of 10KB buffers to prevent GC death at high TPS
	payloadPool = sync.Pool{
		New: func() any {
			b := make([]byte, 10240)
			return b
		},
	}
)

func main() {
	mode := os.Getenv("MODE")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "50051"
	}

	if mode == "producer" {
		runProducer()
	} else {
		runConsumer(appPort)
	}
}

func runProducer() {
	// Dapr uses DAPR_GRPC_PORT env var automatically
	daprClient, err := client.NewClient()
	if err != nil {
		log.Fatalf("failed to init dapr client: %v", err)
	}
	defer daprClient.Close()

	ctx := context.Background()
	// Strict limit: 15,000 TPS
	limiter := rate.NewLimiter(rate.Limit(15000), 500)

	log.Println("PRODUCER: Targeting 15k TPS...")

	for {
		limiter.Wait(ctx)

		data := payloadPool.Get().([]byte)
		// Fire-and-forget goroutine to maintain throughput
		go func(p []byte) {
			defer payloadPool.Put(p)
			err := daprClient.PublishEvent(ctx, "pulsar-pubsub", "hammer-topic", p)
			if err != nil {
				// At 15k TPS, we don't log every error to avoid IO bottleneck
			}
		}(data)
	}
}

func runConsumer(port string) {
	s, err := daprd.NewService(":" + port)
	if err != nil {
		log.Fatalf("failed to start gRPC service: %v", err)
	}

	sub := &common.Subscription{
		PubsubName: "pulsar-pubsub",
		Topic:      "hammer-topic",
	}

	if err := s.AddTopicEventHandler(sub, func(ctx context.Context, e *common.TopicEvent) (bool, error) {
		// Log nothing to maintain high-speed drain
		return false, nil
	}); err != nil {
		log.Fatalf("error adding topic handler: %v", err)
	}

	log.Printf("CONSUMER: Listening on port %s", port)
	if err := s.Start(); err != nil {
		log.Fatalf("error starting service: %v", err)
	}
}