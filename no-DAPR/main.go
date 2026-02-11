package main

import (
	"context"
	"crypto/rand"
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
	maxConcurrency = 15000
	statsInterval  = 10 * time.Second
	payloadSize    = 10240 // 10KB

	// Batch configuration to stay under 5MB limit
	maxBatchMessages = 400             // 400 × 10KB = 4MB raw
	maxBatchSize     = 4 * 1024 * 1024 // 4MB limit
)

var (
	payloadPool = sync.Pool{
		New: func() any {
			p := make([]byte, payloadSize)
			_, err := rand.Read(p)
			if err != nil {
				for i := range p {
					p[i] = byte(i % 256)
				}
			}
			return p
		},
	}
	messageCount uint64
	errorCount   uint64
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	os.Setenv("GOGC", "200")

	mode := os.Getenv("MODE")
	appPort := getEnv("APP_PORT", "50051")
	pulsarURL := getEnv("PULSAR_URL", "pulsar://pulsar-proxy.pulsar.svc.cluster.local:6650")
	topic := getEnv("TOPIC_NAME", "persistent://default/perf-test/perf-testing-topic")
	token := os.Getenv("PULSAR_AUTH_TOKEN")

	clientOpts := pulsar.ClientOptions{
		URL:                     pulsarURL,
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       30 * time.Second,
		MaxConnectionsPerBroker: 5,
		MemoryLimitBytes:        512 * 1024 * 1024,
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
		log.Printf("STARTING PRODUCER - TARGET 15K TPS - Payload: %d bytes, MaxBatch: %d msgs",
			payloadSize, maxBatchMessages)
		go startHealthServer(appPort)
		runProducer(client, topic)
	} else {
		log.Printf("STARTING CONSUMER")
		runConsumer(client, topic)
	}
}

func runProducer(client pulsar.Client, topic string) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		MaxPendingMessages:      50000,
		BatchingMaxPublishDelay: 1 * time.Millisecond,
		BatchingMaxMessages:     maxBatchMessages, // 400 messages
		BatchingMaxSize:         maxBatchSize,     // 4MB
		CompressionType:         pulsar.LZ4,
		DisableBlockIfQueueFull: false,
		SendTimeout:             30 * time.Second,
		Properties: map[string]string{
			"origin": "hammer-pod",
		},
	})
	if err != nil {
		log.Fatalf("Producer Fail: %v", err)
	}
	defer producer.Close()

	// Pre-warm payload pool
	for i := 0; i < 1000; i++ {
		payloadPool.Put(payloadPool.New())
	}

	sem := make(chan struct{}, maxConcurrency)

	// Multiple sender goroutines
	numSenders := runtime.NumCPU() * 2
	if numSenders > 32 {
		numSenders = 32 // Cap at reasonable limit
	}

	var wg sync.WaitGroup

	for i := 0; i < numSenders; i++ {
		wg.Add(1)
		go func(senderID int) {
			defer wg.Done()

			for {
				sem <- struct{}{}
				p := payloadPool.Get().([]byte)

				producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
					Payload: p,
				}, func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
					defer func() { <-sem }()
					payloadPool.Put(p)

					if err != nil {
						atomic.AddUint64(&errorCount, 1)
						// Log specific errors for debugging
						if atomic.LoadUint64(&errorCount)%100 == 0 {
							log.Printf("Send error (total: %d): %v", atomic.LoadUint64(&errorCount), err)
						}
					} else {
						atomic.AddUint64(&messageCount, 1)
					}
				})
			}
		}(i)
	}

	wg.Wait()
}

func runConsumer(client pulsar.Client, topic string) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "hammer-shared-sub",
		Type:                        pulsar.Shared,
		ReceiverQueueSize:           10000,
		NackRedeliveryDelay:         1 * time.Second,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	if err != nil {
		log.Fatalf("Consumer Fail: %v", err)
	}
	defer consumer.Close()

	// Multiple consumer goroutines
	numConsumers := runtime.NumCPU()
	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, err := consumer.Receive(context.Background())
				if err != nil {
					atomic.AddUint64(&errorCount, 1)
					continue
				}
				atomic.AddUint64(&messageCount, 1)
				consumer.Ack(msg)
			}
		}()
	}

	wg.Wait()
}

func logStats() {
	ticker := time.NewTicker(statsInterval)
	start := time.Now()
	var lastCount uint64
	var lastTime time.Time = time.Now()

	for range ticker.C {
		curr := atomic.LoadUint64(&messageCount)
		errs := atomic.LoadUint64(&errorCount)
		elapsed := time.Since(start).Seconds()

		now := time.Now()
		instantTPS := float64(curr-lastCount) / now.Sub(lastTime).Seconds()
		avg := float64(curr) / elapsed
		throughputMBs := (instantTPS * payloadSize) / (1024 * 1024)

		errorRate := float64(0)
		if curr > 0 {
			errorRate = float64(errs) / float64(curr+errs) * 100
		}

		log.Printf("[STATS] Instant TPS: %.0f | Avg TPS: %.0f | Throughput: %.2f MB/s | Total: %d | Errors: %d (%.2f%%) | CPU: %d",
			instantTPS, avg, throughputMBs, curr, errs, errorRate, runtime.NumCPU())

		lastCount = curr
		lastTime = now
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
