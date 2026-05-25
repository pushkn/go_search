package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type event struct {
	EventID   string    `json:"event_id"`
	Query     string    `json:"query"`
	UserID    string    `json:"user_id"`
	SessionID string    `json:"session_id"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	var (
		broker   = flag.String("broker", "localhost:9092", "Kafka broker")
		topic    = flag.String("topic", "search.events", "Kafka topic")
		rps      = flag.Int("rps", 1000, "events per second")
		queries  = flag.Int("queries", 5000, "unique queries")
		users    = flag.Int("users", 50000, "unique users")
		botShare = flag.Float64("bot-share", 0.0, "fraction of bot traffic (0..1)")
		botQuery = flag.String("bot-query", "fake-trend", "query that bots spam")
		bots     = flag.Int("bots", 20, "number of bot users")
		duration = flag.Duration("duration", 30*time.Second, "how long to run")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	writer := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	defer writer.Close()

	logger.Info("load generator started",
		"broker", *broker,
		"topic", *topic,
		"rps", *rps,
		"queries", *queries,
		"users", *users,
		"bot_share", *botShare,
		"duration", *duration,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	var sent, failed int64
	const tickInterval = 100 * time.Millisecond
	perTick := *rps / 10
	if perTick < 1 {
		perTick = 1
	}

	const workers = 4
	jobs := make(chan event, perTick*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range jobs {
				b, _ := json.Marshal(ev)
				if err := writer.WriteMessages(ctx, kafka.Message{Value: b}); err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				atomic.AddInt64(&sent, 1)
			}
		}()
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(rng, 1.2, 1.0, uint64(*queries-1))

	statTicker := time.NewTicker(time.Second)
	defer statTicker.Stop()
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	startedAt := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-statTicker.C:
			s := atomic.LoadInt64(&sent)
			f := atomic.LoadInt64(&failed)
			logger.Info("progress", "sent", s, "failed", f, "elapsed", time.Since(startedAt).Round(time.Second))
		case <-tick.C:
			for i := 0; i < perTick; i++ {
				ev := generateEvent(rng, zipf, *queries, *users, *botShare, *botQuery, *bots)
				select {
				case jobs <- ev:
				case <-ctx.Done():
					break loop
				}
			}
		}
	}

	close(jobs)
	wg.Wait()

	logger.Info("done", "sent", atomic.LoadInt64(&sent), "failed", atomic.LoadInt64(&failed))
}

func generateEvent(rng *rand.Rand, zipf *rand.Zipf, queries, users int, botShare float64, botQuery string, botCount int) event {
	now := time.Now().UTC()
	if rng.Float64() < botShare {
		botID := rng.Intn(botCount)
		return event{
			EventID:   uuid.NewString(),
			Query:     botQuery,
			UserID:    fmt.Sprintf("bot-%d", botID),
			SessionID: fmt.Sprintf("bot-session-%d", botID),
			Timestamp: now,
		}
	}
	queryID := int(zipf.Uint64())
	userID := rng.Intn(users)
	return event{
		EventID:   uuid.NewString(),
		Query:     fmt.Sprintf("query-%d", queryID),
		UserID:    fmt.Sprintf("user-%d", userID),
		SessionID: fmt.Sprintf("session-%d", userID),
		Timestamp: now,
	}
}
