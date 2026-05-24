package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pushkn/go_search/internal/config"
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

	logger.Info("service started, waiting for shutdown signal")

	<-ctx.Done()

	logger.Info("shutdown signal received, stopping")

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
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}

	logger.Info("topic ensured", "name", cfg.KafkaTopic)
	return nil
}
