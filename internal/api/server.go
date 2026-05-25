package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pushkn/go_search/internal/snapshot"
	"github.com/pushkn/go_search/internal/stoplist"
)

type Config struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type Server struct {
	cfg      Config
	logger   *slog.Logger
	builder  *snapshot.Builder
	stoplist *stoplist.StopList
	srv      *http.Server
}

func NewServer(cfg Config, logger *slog.Logger, builder *snapshot.Builder, sl *stoplist.StopList) *Server {
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}

	s := &Server{
		cfg:      cfg,
		logger:   logger,
		builder:  builder,
		stoplist: sl,
	}

	r := chi.NewRouter()
	r.Use(requestID)
	r.Use(s.recovery)
	r.Use(s.metricsMW)
	r.Use(s.logging)

	r.Handle("/metrics", promhttp.Handler())
	r.Get("/healthz", s.handleHealth)
	r.Get("/readyz", s.handleReady)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/trending", s.handleTrending)
		r.Get("/stoplist", s.handleStopListGet)
		r.Post("/stoplist", s.handleStopListAdd)
		r.Delete("/stoplist/{word}", s.handleStopListDelete)
	})

	s.srv = &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("http server starting", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	s.logger.Info("http server shutting down")
	return s.srv.Shutdown(shutdownCtx)
}
