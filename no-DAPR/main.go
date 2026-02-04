package main

import (
	"context"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	maxConcurrency = 8000 // Higher concurrency for async ceiling
	statsInterval  = 10 * time.Second
	payloadSize    = 10240 // 10KB
)

var (
	payloadPool = sync.Pool{
		New: func() any { return make([]byte, payloadSize) },
	}
	messageCount uint64
	errorCount   uint64
)

func main() {
	// Maximize CPU utilization
	runtime.GOMAXPROCS(runtime.NumCPU())

	mode := os.Getenv("MODE")
	appPort := getEnv("APP_PORT", "50051")
	pulsarURL := getEnv("PULSAR_URL", "pulsar://pulsar-proxy.pulsar.svc.cluster.local:6650")
	topic := getEnv("TOPIC_NAME", "hammer-topic")
	token := os.Getenv("PULSAR_AUTH_TOKEN")

	// 1. Initialize High-Performance Native Client
	clientOpts := pulsar.ClientOptions{
		URL:               pulsarURL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	}

	if token != "" {
		clientOpts.Authentication = pulsar.NewAuthenticationToken(token)
	}

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		log.Fatalf("Pulsar Client Fail: %v", err)
	}
	defer client.Close()

	go logStats()

	if mode == "producer" {
		log.Printf("STARTING NATIVE PRODUCER - TARGET 15K+ TPS")
		go startHealthServer(appPort)
		runProducer(client, topic)
	} else {
		log.Printf("STARTING NATIVE CONSUMER")
		runConsumer(client, topic)
	}
}

func runProducer(client pulsar.Client, topic string) {
	// 2. High-Throughput Producer Settings
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		MaxPendingMessages:      20000,                // Large internal buffer
		BatchingMaxPublishDelay: 5 * time.Millisecond, // Flush quickly but batched
		BatchingMaxMessages:     2000,                 // Group up to 2k messages per send
		BatchingMaxSize:         4 * 1024 * 1024,      // 4MB per batch
		CompressionType:         pulsar.LZ4,           // Lowest CPU overhead
		Properties: map[string]string{
			"origin": "hammer-pod",
		},
	})
	if err != nil {
		log.Fatalf("Producer Fail: %v", err)
	}
	defer producer.Close()

	// 3. Semaphores to prevent memory runaway
	sem := make(chan struct{}, maxConcurrency)

	for {
		sem <- struct{}{}

		p := payloadPool.Get().([]byte)

		// Async Send is non-blocking
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: p,
		}, func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
			defer func() { <-sem }()
			payloadPool.Put(p) // Return to pool immediately

			if err != nil {
				atomic.AddUint64(&errorCount, 1)
			} else {
				atomic.AddUint64(&messageCount, 1)
			}
		})
	}
}

func runConsumer(client pulsar.Client, topic string) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "hammer-shared-sub",
		Type:              pulsar.Shared,
		ReceiverQueueSize: 5000, // High pre-fetch buffer
	})
	if err != nil {
		log.Fatalf("Consumer Fail: %v", err)
	}
	defer consumer.Close()

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			atomic.AddUint64(&errorCount, 1)
			continue
		}

		atomic.AddUint64(&messageCount, 1)
		consumer.Ack(msg)
	}
}

// Stats and Utility
func logStats() {
	ticker := time.NewTicker(statsInterval)
	start := time.Now()
	var lastCount uint64

	for range ticker.C {
		curr := atomic.LoadUint64(&messageCount)
		errs := atomic.LoadUint64(&errorCount)
		elapsed := time.Since(start).Seconds()

		tps := float64(curr-lastCount) / statsInterval.Seconds()
		avg := float64(curr) / elapsed

		log.Printf("[STATS] TPS: %.0f | Avg: %.0f | Total: %d | Errors: %d", tps, avg, curr, errs)
		lastCount = curr
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func startHealthServer(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Health Port Busy: %v", err)
	}
	gs := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)
	gs.Serve(lis)
}
