package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pushkn/go_search/internal/domain"
	"github.com/pushkn/go_search/internal/metrics"
)

type Deduper interface {
	Seen(userID, sessionID, query string) bool
}

type AnomalyObserver interface {
	Observe(query string, now time.Time)
}

type WindowAdder interface {
	Add(query string, ts time.Time, now time.Time) bool
}

type Config struct {
	Brokers    []string
	Topic      string
	GroupID    string
	Workers    int
	BufferSize int
}

type Consumer struct {
	cfg     Config
	logger  *slog.Logger
	deduper Deduper
	anomaly AnomalyObserver
	window  WindowAdder
	reader  *kafka.Reader
}

func New(cfg Config, logger *slog.Logger, deduper Deduper, anomaly AnomalyObserver, window WindowAdder) *Consumer {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: time.Second,
	})
	return &Consumer{
		cfg:     cfg,
		logger:  logger,
		deduper: deduper,
		anomaly: anomaly,
		window:  window,
		reader:  reader,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	jobs := make(chan kafka.Message, c.cfg.BufferSize)
	var wg sync.WaitGroup

	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.worker(ctx, id, jobs)
		}(i)
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := c.reader.Stats()
				metrics.KafkaConsumerLag.Set(float64(stats.Lag))
			}
		}
	}()
	
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			close(jobs)
			wg.Wait()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		select {
		case jobs <- msg:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil
		}
	}
}

func (c *Consumer) worker(ctx context.Context, id int, jobs <-chan kafka.Message) {
	for msg := range jobs {
		c.process(msg)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.logger.Error("commit failed", "worker", id, "error", err)
		}
	}
}

func (c *Consumer) process(msg kafka.Message) {
	start := time.Now()
	defer func() {
		metrics.EventProcessingDuration.Observe(time.Since(start).Seconds())
	}()

	var ev domain.SearchEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		metrics.EventsConsumed.WithLabelValues("dropped_unmarshal").Inc()
		c.logger.Warn("unmarshal failed", "error", err, "offset", msg.Offset)
		return
	}
	ev.Normalize()

	now := time.Now().UTC()
	if err := ev.Validate(now); err != nil {
		metrics.EventsConsumed.WithLabelValues("dropped_validation").Inc()
		c.logger.Debug("invalid event", "error", err, "query", ev.Query)
		return
	}

	if c.deduper.Seen(ev.UserID, ev.SessionID, ev.Query) {
		metrics.EventsConsumed.WithLabelValues("dropped_dedup").Inc()
		return
	}

	c.anomaly.Observe(ev.Query, now)
	if !c.window.Add(ev.Query, ev.Timestamp, now) {
		metrics.EventsConsumed.WithLabelValues("dropped_stale").Inc()
		return
	}
	metrics.EventsConsumed.WithLabelValues("ok").Inc()
}
