package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metricsServer struct {
	server *http.Server
	done   chan error
}

func startMetricsServer(opts parseTelemetryOptions) (*metricsServer, error) {
	address := strings.TrimSpace(opts.MetricsListenAddress)
	if address == "" {
		return nil, nil
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen metrics endpoint %q: %w", address, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", newMetricsHandler())
	server := &http.Server{Handler: mux}
	metrics := &metricsServer{
		server: server,
		done:   make(chan error, 1),
	}
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		metrics.done <- err
	}()
	return metrics, nil
}

func (s *metricsServer) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	shutdownErr := s.server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = s.server.Close()
	}
	serveErr := <-s.done
	if shutdownErr != nil {
		return shutdownErr
	}
	return serveErr
}

func newMetricsHandler() http.Handler {
	return promhttp.HandlerFor(newMetricsRegistry(), promhttp.HandlerOpts{})
}

func newMetricsRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "drain_cluster_build_info",
		Help: "Build information for the Drain cluster binary.",
	}, []string{"version", "commit"})
	buildInfo.WithLabelValues(buildVersion, buildCommit).Set(1)
	registry.MustRegister(buildInfo)
	return registry
}
