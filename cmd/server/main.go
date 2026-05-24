package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pushkn/go_search/internal/api"
	"github.com/pushkn/go_search/internal/config"
	"github.com/pushkn/go_search/internal/consumer"
	"github.com/pushkn/go_search/internal/pipeline"
	"github.com/pushkn/go_search/internal/snapshot"
	"github.com/pushkn/go_search/internal/stoplist"
	"github.com/pushkn/go_search/internal/topk"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = os.Stderr.WriteString("config load failed: " + err.Error() + "\n")
		os.Exit(1)
	}

	logger := newLogger(cfg)
	logger.Info("starting trending-search service",
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_topic", cfg.KafkaTopic,
		"http_port", cfg.HTTPPort,
		"window_size", cfg.WindowSize,
		"bucket_duration", cfg.BucketDuration,
		"bucket_count", cfg.BucketCount(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := ensureTopic(ctx, logger, cfg); err != nil {
		logger.Error("failed to ensure topic", "error", err)
		os.Exit(1)
	}

	window := topk.NewWindow(topk.Config{
		WindowSize:     cfg.WindowSize,
		BucketDuration: cfg.BucketDuration,
		Capacity:       cfg.SpaceSavingK,
	})

	deduper := pipeline.NewDeduper(pipeline.DedupConfig{
		WindowSize:       30 * time.Second,
		Rotations:        3,
		ExpectedElements: 1_000_000,
		FPRate:           0.01,
	})
	deduper.Start(ctx)

	anomaly := pipeline.NewAnomaly(pipeline.AnomalyConfig{
		BucketDuration: cfg.BucketDuration,
		Alpha:          0.3,
		Threshold:      5.0,
		MinAge:         60 * time.Second,
		GCInterval:     1 * time.Minute,
		GCTTL:          10 * time.Minute,
	})
	anomaly.Start(ctx)

	stopList := stoplist.New()

	builder := snapshot.NewBuilder(window, anomaly, stopList, snapshot.Config{
		Interval:       cfg.SnapshotInterval,
		MaxSize:        cfg.SnapshotMaxSize,
		AnomalyPenalty: 0.1,
	})
	builder.Start(ctx)

	kafkaConsumer := consumer.New(consumer.Config{
		Brokers:    cfg.KafkaBrokers,
		Topic:      cfg.KafkaTopic,
		GroupID:    cfg.KafkaGroupID,
		Workers:    runtime.NumCPU(),
		BufferSize: 1024,
	}, logger, deduper, anomaly, window)

	server := api.NewServer(api.Config{
		Port: cfg.HTTPPort,
	}, logger, builder, stopList)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := kafkaConsumer.Run(ctx); err != nil {
			logger.Error("consumer stopped with error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Run(ctx); err != nil {
			logger.Error("http server stopped with error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rotateTicker := time.NewTicker(cfg.BucketDuration)
		defer rotateTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-rotateTicker.C:
				window.Rotate(t)
			}
		}
	}()

	logger.Info("service started, waiting for shutdown signal")
	<-ctx.Done()
	logger.Info("shutdown signal received, stopping")

	wg.Wait()
	logger.Info("service stopped")
}

func newLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.LogFormat == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}

func ensureTopic(ctx context.Context, logger *slog.Logger, cfg *config.Config) error {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second}

	conn, err := dialer.DialContext(ctx, "tcp", cfg.KafkaBrokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             cfg.KafkaTopic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		var kafkaErr kafka.Error
		if !errors.As(err, &kafkaErr) || kafkaErr != kafka.TopicAlreadyExists {
			return err
		}
	}

	logger.Info("topic ensured", "name", cfg.KafkaTopic)
	return nil
}
