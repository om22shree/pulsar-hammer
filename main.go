package main

import (
	"context"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/grpc"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	poolSize    = 10     // Number of concurrent gRPC connections to Dapr
	logInterval = 100000 // Only log every 100k messages to save CPU
)

var (
	payloadPool = sync.Pool{
		New: func() any { return make([]byte, 10240) },
	}
	messageCount uint64
)

func main() {
	mode := os.Getenv("MODE")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "50051"
	}

	if mode == "producer" {
		log.Printf("PRODUCER: Initializing with %d parallel clients", poolSize)
		// 1. Unblock sidecar
		go startProducerHealthCheck(appPort)
		time.Sleep(2 * time.Second)
	
		runProducer()
	} else {
		log.Printf("CONSUMER: Starting Dapr Service on %s", appPort)
		runConsumer(appPort)
	}
}

func startProducerHealthCheck(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Producer listener failed: %v", err)
	}
	gs := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)
	if err := gs.Serve(lis); err != nil {
		log.Printf("Producer health server stopped: %v", err)
	}
}

func runProducer() {
	// Create a pool of clients to break the gRPC single-connection limit
	var clients []client.Client
	for i := 0; i < poolSize; i++ {
		c, err := client.NewClient()
		if err != nil {
			log.Fatalf("failed to init dapr client %d: %v", i, err)
		}
		clients = append(clients, c)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	limiter := rate.NewLimiter(rate.Limit(15000), 2000)
	sem := make(chan struct{}, 2000) // Allow more burst concurrency

	for {
		limiter.Wait(context.Background())
		sem <- struct{}{}
		
		go func(id uint64) {
			defer func() { <-sem }()
			p := payloadPool.Get().([]byte)
			defer payloadPool.Put(p)

			// Rotate through the client pool
			c := clients[id%poolSize]

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			err := c.PublishEvent(ctx, "pulsar-pubsub", "hammer-topic", p)
			if err == nil {
				newVal := atomic.AddUint64(&messageCount, 1)
				if newVal%logInterval == 0 {
					log.Printf("PRODUCER: Sent %d messages total", newVal)
				}
			}
		}(atomic.LoadUint64(&messageCount))
	}
}

func runConsumer(port string) {
	s, err := daprd.NewService(":" + port)
	if err != nil {
		log.Fatalf("failed to create dapr service: %v", err)
	}

	sub := &common.Subscription{
		PubsubName: "pulsar-pubsub",
		Topic:      "hammer-topic",
	}

	err = s.AddTopicEventHandler(sub, func(ctx context.Context, e *common.TopicEvent) (bool, error) {
		newVal := atomic.AddUint64(&messageCount, 1)
		if newVal%logInterval == 0 {
			log.Printf("CONSUMER: Received %d messages total", newVal)
		}
		return false, nil
	})
	if err != nil {
		log.Fatalf("error adding topic handler: %v", err)
	}

	if err := s.Start(); err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}
}