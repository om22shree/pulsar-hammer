package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
	poolSize    = 10     // Concurrent gRPC connections to Dapr
	logInterval = 100000 // Log every 100k messages
)

var (
	payloadPool = sync.Pool{
		New: func() any { return make([]byte, 10240) }, // 10KB payload
	}
	messageCount uint64
	pubsubName string
	topicFQTN  string
	rateLimit  float64
)

func main() {
	mode := os.Getenv("MODE")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "50051"
	}

	tenant := getEnv("PULSAR_TENANT", "public")
	namespace := getEnv("PULSAR_NAMESPACE", "default")
	topic := getEnv("TOPIC_NAME", "hammer-topic")
	topicFQTN = fmt.Sprintf("persistent://%s/%s/%s", tenant, namespace, topic)
	pubsubName = getEnv("PUBSUB_NAME", "pulsar-pubsub")
	rateLimitStr := getEnv("RATE_LIMIT", "15000")
	var err error
	rateLimit, err = strconv.ParseFloat(rateLimitStr, 64)
	if err != nil {
		rateLimit = 15000
	}

	if mode == "producer" {
		log.Printf("PRODUCER: Starting. Target: %s | Rate: %.0f TPS", topicFQTN, rateLimit)
		go startHealthServer(appPort)
		log.Printf("PRODUCER: Waiting 10s for sidecar stabilization...")
		time.Sleep(10 * time.Second)
		runProducer()
	} else {
		log.Printf("CONSUMER: Starting on %s. Target: %s", appPort, topicFQTN)
		runConsumer(appPort)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func startHealthServer(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Health listener failed: %v", err)
	}
	gs := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)
	log.Printf("Health server listening on %s", port)
	if err := gs.Serve(lis); err != nil {
		log.Printf("Health server error: %v", err)
	}
}

func runProducer() {
	var clients []client.Client
	for i := 0; i < poolSize; i++ {
		c, err := client.NewClient()
		if err != nil {
			log.Fatalf("failed to init dapr client %d: %v", i, err)
		}
		clients = append(clients, c)
		time.Sleep(100 * time.Millisecond)
	}

	limiter := rate.NewLimiter(rate.Limit(rateLimit), 2000)
	sem := make(chan struct{}, 2000) // Concurrency throttle
	for {
		limiter.Wait(context.Background())
		sem <- struct{}{}
		
		go func(id uint64) {
			defer func() { <-sem }()
			p := payloadPool.Get().([]byte)
			defer payloadPool.Put(p)

			c := clients[id%poolSize]
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := c.PublishEvent(ctx, pubsubName, topicFQTN, p)
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
		PubsubName: pubsubName,
		Topic:      topicFQTN,
	}

	err = s.AddTopicEventHandler(sub, func(ctx context.Context, e *common.TopicEvent) (bool, error) {
		newVal := atomic.AddUint64(&messageCount, 1)
		if newVal%logInterval == 0 {
			log.Printf("CONSUMER: Received %d messages total", newVal)
		}
		return false, nil
	})
	
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}
}