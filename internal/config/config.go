package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	HTTPPort string

	WindowSize     time.Duration
	BucketDuration time.Duration
	SpaceSavingK   int

	SnapshotInterval time.Duration
	SnapshotMaxSize  int

	LogLevel  string
	LogFormat string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers: splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "search.events"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "trending-search"),
		HTTPPort:     getEnv("HTTP_PORT", "8080"),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
		LogFormat:    getEnv("LOG_FORMAT", "text"),
	}

	var err error
	if cfg.WindowSize, err = getEnvDuration("WINDOW_SIZE", 5*time.Minute); err != nil {
		return nil, err
	}
	if cfg.BucketDuration, err = getEnvDuration("BUCKET_DURATION", 10*time.Second); err != nil {
		return nil, err
	}
	if cfg.SpaceSavingK, err = getEnvInt("SPACE_SAVING_K", 10000); err != nil {
		return nil, err
	}
	if cfg.SnapshotInterval, err = getEnvDuration("SNAPSHOT_INTERVAL", 1*time.Second); err != nil {
		return nil, err
	}
	if cfg.SnapshotMaxSize, err = getEnvInt("SNAPSHOT_MAX_SIZE", 1000); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKERS must not be empty")
	}
	if c.KafkaTopic == "" {
		return errors.New("KAFKA_TOPIC must not be empty")
	}
	if c.WindowSize <= 0 {
		return errors.New("WINDOW_SIZE must be positive")
	}
	if c.BucketDuration <= 0 {
		return errors.New("BUCKET_DURATION must be positive")
	}
	if c.WindowSize%c.BucketDuration != 0 {
		return fmt.Errorf(
			"WINDOW_SIZE (%s) must be a multiple of BUCKET_DURATION (%s)",
			c.WindowSize, c.BucketDuration,
		)
	}
	if c.SpaceSavingK <= 0 {
		return errors.New("SPACE_SAVING_K must be positive")
	}
	if c.SnapshotInterval <= 0 {
		return errors.New("SNAPSHOT_INTERVAL must be positive")
	}
	if c.SnapshotMaxSize <= 0 {
		return errors.New("SNAPSHOT_MAX_SIZE must be positive")
	}
	return nil
}

func (c *Config) BucketCount() int {
	return int(c.WindowSize / c.BucketDuration)
}

func getEnv(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return defaultValue, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid int for %s: %w", key, err)
	}
	return n, nil
}

func getEnvDuration(key string, defaultValue time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %s: %w", key, err)
	}
	return d, nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
