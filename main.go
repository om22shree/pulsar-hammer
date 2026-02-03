package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	poolSize        = 300
	logInterval     = 500000
	maxConcurrency  = 5000
	publishTimeout  = 10 * time.Second
	clientInitDelay = 20 * time.Millisecond
	statsInterval   = 10 * time.Second
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

	if mode == "producer" {
		log.Printf("PRODUCER: Full throttle mode")
		log.Printf("Target: %s | Concurrency: %d | Pool: %d", topicFQTN, maxConcurrency, poolSize)
		go startHealthServer(appPort)
		go logStats()
		log.Printf("Waiting 10s for sidecar stabilization")
		time.Sleep(10 * time.Second)
		runProducer()
	} else {
		log.Printf("CONSUMER: Starting on %s", appPort)
		go logStats()
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
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	lastCount := uint64(0)
	lastErrors := uint64(0)
	lastTimeouts := uint64(0)
	startTime := time.Now()

	for range ticker.C {
		current := atomic.LoadUint64(&messageCount)
		errors := atomic.LoadUint64(&errorCount)
		timeouts := atomic.LoadUint64(&timeoutCount)

		intervalSeconds := statsInterval.Seconds()
		tps := float64(current-lastCount) / intervalSeconds
		newErrors := errors - lastErrors
		newTimeouts := timeouts - lastTimeouts

		elapsed := time.Since(startTime).Seconds()
		avgTPS := float64(current) / elapsed

		log.Printf("STATS: TPS=%.0f Avg=%.0f Total=%d Errors=%d(+%d) Timeouts=%d(+%d)",
			tps, avgTPS, current, errors, newErrors, timeouts, newTimeouts)

		lastCount = current
		lastErrors = errors
		lastTimeouts = timeouts
	}
}

type ClientPool struct {
	clients []client.Client
	index   uint64
}

func NewClientPool(size int) *ClientPool {
	pool := &ClientPool{
		clients: make([]client.Client, 0, size),
	}

	log.Printf("Initializing %d Dapr clients", size)

	var wg sync.WaitGroup
	clientChan := make(chan client.Client, size)
	errChan := make(chan error, size)

	batchSize := 20
	for i := 0; i < size; i += batchSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + batchSize
			if end > size {
				end = size
			}
			for j := start; j < end; j++ {
				c, err := client.NewClient()
				if err != nil {
					errChan <- err
					return
				}
				clientChan <- c
				time.Sleep(clientInitDelay)
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(clientChan)
		close(errChan)
	}()

	for c := range clientChan {
		pool.clients = append(pool.clients, c)
	}

	if len(pool.clients) < size {
		log.Fatalf("Failed to initialize all clients. Got %d/%d", len(pool.clients), size)
	}

	log.Printf("All %d clients initialized", len(pool.clients))
	return pool
}

func (p *ClientPool) Get() client.Client {
	idx := atomic.AddUint64(&p.index, 1)
	return p.clients[idx%uint64(len(p.clients))]
}

func (p *ClientPool) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

func runProducer() {
	pool := NewClientPool(poolSize)
	defer pool.Close()

	sem := make(chan struct{}, maxConcurrency)

	log.Printf("Starting full throttle producer")

	for {
		sem <- struct{}{}

		go func() {
			defer func() { <-sem }()

			c := pool.Get()
			p := payloadPool.Get().([]byte)
			ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)

			err := c.PublishEvent(ctx, pubsubName, topicFQTN, p)

			cancel()
			payloadPool.Put(p)

			if err == nil {
				newVal := atomic.AddUint64(&messageCount, 1)
				if newVal%logInterval == 0 {
					log.Printf("PRODUCER: Sent %d messages", newVal)
				}
			} else {
				atomic.AddUint64(&errorCount, 1)
				if ctx.Err() == context.DeadlineExceeded {
					atomic.AddUint64(&timeoutCount, 1)
				}
			}
		}()
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
			"maxConcurrentHandlers": "5000",
		},
	}

	msgChan := make(chan *common.TopicEvent, 50000)

	numProcessors := 500
	log.Printf("Starting %d message processors", numProcessors)
	for i := 0; i < numProcessors; i++ {
		go func() {
			for range msgChan {
				newVal := atomic.AddUint64(&messageCount, 1)
				if newVal%logInterval == 0 {
					log.Printf("CONSUMER: Received %d messages", newVal)
				}
			}
		}()
	}

	err = s.AddTopicEventHandler(sub, func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		select {
		case msgChan <- e:
		default:
			atomic.AddUint64(&messageCount, 1)
		}
		return false, nil
	})

	if err != nil {
		log.Fatalf("Failed to add handler: %v", err)
	}

	log.Printf("Starting consumer service on port %s", appPort)
	if err := s.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
}
