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
	poolSize        = 100              // Connection pool size
	logInterval     = 100000           // Log every 100k messages
	maxConcurrency  = 1000             // Max in-flight messages
	publishTimeout  = 20 * time.Second // Timeout per publish
	batchSize       = 100              // Messages per batch goroutine
	clientInitDelay = 50 * time.Millisecond
)

var (
	payloadPool = sync.Pool{
		New: func() any { return make([]byte, 10240) },
	}
	messageCount uint64
	errorCount   uint64
	timeoutCount uint64
	pubsubName   string
	topicFQTN    string
	rateLimit    float64
)

func main() {
	mode := os.Getenv("MODE")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "50051"
	}

	topic := getEnv("TOPIC_NAME", "hammer-topic")
	topicFQTN = fmt.Sprintf("%s", topic)
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
		go logStats()
		log.Printf("PRODUCER: Waiting 10s for sidecar stabilization...")
		time.Sleep(10 * time.Second)
		runProducer()
	} else {
		log.Printf("CONSUMER: Starting on %s. Target: %s", appPort, topicFQTN)
		go startHealthServer(appPort)
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

func logStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastCount := uint64(0)
	lastErrors := uint64(0)
	lastTimeouts := uint64(0)

	for range ticker.C {
		current := atomic.LoadUint64(&messageCount)
		errors := atomic.LoadUint64(&errorCount)
		timeouts := atomic.LoadUint64(&timeoutCount)

		tps := float64(current-lastCount) / 10.0
		newErrors := errors - lastErrors
		newTimeouts := timeouts - lastTimeouts

		log.Printf("STATS: TPS=%.1f | Total=%d | Errors=%d (+%d) | Timeouts=%d (+%d)",
			tps, current, errors, newErrors, timeouts, newTimeouts)

		lastCount = current
		lastErrors = errors
		lastTimeouts = timeouts
	}
}

type ClientPool struct {
	clients []client.Client
	index   uint64
	mu      sync.RWMutex
}

func NewClientPool(size int) *ClientPool {
	pool := &ClientPool{
		clients: make([]client.Client, 0, size),
	}

	log.Printf("Initializing %d Dapr clients...", size)
	for i := 0; i < size; i++ {
		c, err := client.NewClient()
		if err != nil {
			log.Fatalf("Failed to init dapr client %d: %v", i, err)
		}
		pool.clients = append(pool.clients, c)

		if (i+1)%10 == 0 {
			log.Printf("Initialized %d/%d clients", i+1, size)
		}
		time.Sleep(clientInitDelay)
	}
	log.Printf("All %d clients initialized successfully", size)
	return pool
}

func (p *ClientPool) Get() client.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	idx := atomic.AddUint64(&p.index, 1)
	return p.clients[idx%uint64(len(p.clients))]
}

func (p *ClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		c.Close()
	}
}

func runProducer() {
	pool := NewClientPool(poolSize)
	defer pool.Close()

	// Rate limiter with minimal burst
	burstSize := 200
	limiter := rate.NewLimiter(rate.Limit(rateLimit), burstSize)

	// Semaphore for concurrency control
	sem := make(chan struct{}, maxConcurrency)

	// Worker pool pattern - batch processing
	numWorkers := maxConcurrency / batchSize
	workChan := make(chan struct{}, numWorkers*2)

	log.Printf("Starting %d workers, each processing %d messages", numWorkers, batchSize)

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		go worker(pool, sem, workChan)
	}

	// Main loop - distribute work
	for {
		select {
		case workChan <- struct{}{}:
			// Work dispatched
		default:
			// Channel full, apply backpressure
			time.Sleep(time.Millisecond)
		}

		// Rate limiting
		if err := limiter.Wait(context.Background()); err != nil {
			log.Printf("Rate limiter error: %v", err)
		}
	}
}

func worker(pool *ClientPool, sem chan struct{}, workChan chan struct{}) {
	for range workChan {
		// Acquire semaphore slot
		sem <- struct{}{}

		// Get client from pool
		c := pool.Get()

		// Get payload from pool
		p := payloadPool.Get().([]byte)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)

		// Publish message
		err := c.PublishEvent(ctx, pubsubName, topicFQTN, p)

		// Cleanup
		cancel()
		payloadPool.Put(p)
		<-sem

		// Track results
		if err == nil {
			newVal := atomic.AddUint64(&messageCount, 1)
			if newVal%logInterval == 0 {
				log.Printf("PRODUCER: Sent %d messages total", newVal)
			}
		} else {
			atomic.AddUint64(&errorCount, 1)
			if ctx.Err() == context.DeadlineExceeded {
				atomic.AddUint64(&timeoutCount, 1)
			}

			// Log sampling (1 in 100 errors)
			if atomic.LoadUint64(&errorCount)%100 == 1 {
				log.Printf("WARN: Publish error: %v", err)
			}
		}
	}
}

func runConsumer(appPort string) {
	s, err := daprd.NewService(":" + appPort)
	if err != nil {
		log.Fatalf("Failed to create dapr service: %v", err)
	}

	sub := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      topicFQTN,
		Route:      "/events",
		Metadata: map[string]string{
			"maxConcurrentHandlers": "1000", // Parallel processing
		},
	}

	// Use a buffered channel for async processing
	msgChan := make(chan *common.TopicEvent, 10000)

	// Start processor goroutines
	numProcessors := 100
	log.Printf("Starting %d message processors", numProcessors)
	for i := 0; i < numProcessors; i++ {
		go func() {
			for range msgChan {
				newVal := atomic.AddUint64(&messageCount, 1)
				if newVal%logInterval == 0 {
					log.Printf("CONSUMER: Received %d messages total", newVal)
				}
			}
		}()
	}

	err = s.AddTopicEventHandler(sub, func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		select {
		case msgChan <- e:
			// Message queued successfully
		default:
			// Buffer full - apply backpressure by processing inline
			atomic.AddUint64(&messageCount, 1)
		}
		return false, nil // Always ACK immediately
	})

	if err != nil {
		log.Fatalf("Failed to add handler: %v", err)
	}

	log.Printf("Starting consumer service on port %s", appPort)
	if err := s.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
}
