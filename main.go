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

var (
	payloadPool = sync.Pool{
		New: func() any { return make([]byte, 10240) },
	}
	publishCount uint64
)

func main() {
	mode := os.Getenv("MODE")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "50051"
	}

	if mode == "producer" {
		log.Printf("PRODUCER: Starting health server on %s", appPort)
		// 1. Manually open port for Producer to unblock Dapr Sidecar
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
		log.Printf("Producer health server error: %v", err)
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
				return
			}
			if atomic.AddUint64(&publishCount, 1)%10000 == 0 {
				log.Printf("Published %d messages", atomic.LoadUint64(&publishCount))
			}
		}()
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
		if atomic.AddUint64(&publishCount, 1)%10000 == 0 {
			log.Printf("Consumed %d messages", atomic.LoadUint64(&publishCount))
		}
		return false, nil
	})
	if err != nil {
		log.Fatalf("error adding topic handler: %v", err)
	}

	// s.Start() will block and handle the port listening
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}
}
