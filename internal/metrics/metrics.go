package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trending_events_consumed_total",
			Help: "Number of events consumed from Kafka by status",
		},
		[]string{"status"},
	)

	EventProcessingDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "trending_event_processing_duration_seconds",
			Help:    "Time spent processing a single event",
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 15),
		},
	)

	EventsMarkedAnomaly = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "trending_events_marked_anomaly_total",
			Help: "Number of queries flagged as anomaly during snapshot build",
		},
	)

	SnapshotSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_snapshot_size",
			Help: "Current size of the trending snapshot",
		},
	)

	SnapshotBuildDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "trending_snapshot_build_duration_seconds",
			Help:    "Time spent building a snapshot",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		},
	)

	SnapshotLastBuilt = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_snapshot_last_built_seconds",
			Help: "Unix timestamp of the last snapshot build",
		},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "trending_http_request_duration_seconds",
			Help:    "HTTP request duration",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
		},
		[]string{"method", "path", "status"},
	)

	HTTPInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_http_requests_in_flight",
			Help: "Number of HTTP requests currently being served",
		},
	)

	KafkaConsumerLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_kafka_consumer_lag",
			Help: "Total Kafka consumer lag across all partitions",
		},
	)

	StopListSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_stoplist_size",
			Help: "Current number of words in the stop list",
		},
	)
)
