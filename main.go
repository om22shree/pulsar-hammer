package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

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
	daprClient, err := client.NewClient()
	if err != nil {
		log.Fatalf("failed to init dapr client: %v", err)
	}
	defer daprClient.Close()

	limiter := rate.NewLimiter(rate.Limit(15000), 1000)

	sem := make(chan struct{}, 1000)

	for {
		limiter.Wait(context.Background())

		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()

			p := payloadPool.Get().([]byte)
			defer payloadPool.Put(p)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := daprClient.PublishEvent(ctx, "pulsar-pubsub", "hammer-topic", p)
			if err != nil {
				log.Printf("Publish error: %v", err)
			}
		}()
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
		return false, nil
	}); err != nil {
		log.Fatalf("error adding topic handler: %v", err)
	}

	log.Printf("CONSUMER: Listening on port %s", port)
	if err := s.Start(); err != nil {
		log.Fatalf("error starting service: %v", err)
	}
}
