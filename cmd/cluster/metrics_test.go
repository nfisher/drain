package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/faceair/drain/internal/parseio"
	a "github.com/gogunit/gunit/hammy"
)

func TestMetricsHandlerExposesBuildInfo(t *testing.T) {
	assert := a.New(t)
	withBuildInfo(t, "1.2.3+abc1234def56", "abc1234def56")

	recorder := httptest.NewRecorder()
	newMetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	assert.Requires(a.Number(recorder.Code).EqualTo(http.StatusOK))
	body := recorder.Body.String()
	assert.Requires(a.String(body).Contains("drain_cluster_build_info"))
	assert.Requires(a.String(body).Contains(`version="1.2.3+abc1234def56"`))
	assert.Requires(a.String(body).Contains(`commit="abc1234def56"`))
	assert.Requires(a.String(body).Contains(" 1\n"))
}

func TestReadParseConfigReadsTelemetry(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	configPath := writeHCLConfig(t, dir, `
telemetry {
  metrics_listen_address = ":9090"
}

pipeline "p" {
  model = "model.json"
  source "file" {
    filename = "target.log"
  }
  sink "jsonl" {}
}
`)

	config, err := readParseConfig(configPath)

	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Telemetry.MetricsListenAddress).EqualTo(":9090"))
	assert.Requires(a.Number(len(config.Pipelines)).EqualTo(1))
}

func TestReadParseConfigRejectsMultipleTelemetryBlocks(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	configPath := writeHCLConfig(t, dir, `
telemetry {
  metrics_listen_address = ":9090"
}

telemetry {
  metrics_listen_address = ":9091"
}

pipeline "p" {
  model = "model.json"
  source "file" {
    filename = "target.log"
  }
  sink "jsonl" {}
}
`)

	_, err := readParseConfig(configPath)

	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("at most one telemetry block"))
}

func TestRunParseGenerateConfigWritesTelemetryHCL(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"parse",
		"-generate-config",
		"-filename", filepath.Join(dir, "missing.log"),
		"-model", filepath.Join(dir, "missing-model.json"),
		"-metrics-listen-address", ":9090",
	}, &stdout, &stderr)

	assert.Requires(a.NilError(err))
	assert.Requires(a.Buffer(&stderr).IsEmpty())
	generated := stdout.String()
	assert.Requires(a.String(generated).Contains("telemetry {"))
	assert.Requires(a.String(generated).Contains(`metrics_listen_address = ":9090"`))

	configPath := writeHCLConfig(t, dir, generated)
	config, err := readParseConfig(configPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Telemetry.MetricsListenAddress).EqualTo(":9090"))
}

func TestRunParseConfigMetricsFlagOverridesTelemetryBlock(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath, logPath := writeMetricsParseInputs(t, dir)
	busyListener := listenTCP(t, "127.0.0.1:0")
	defer busyListener.Close()
	configPath := writeHCLConfig(t, dir, `
telemetry {
  metrics_listen_address = "`+busyListener.Addr().String()+`"
}

pipeline "p" {
  model = "`+modelPath+`"
  source "file" {
    filename = "`+logPath+`"
  }
  sink "jsonl" {}
}
`)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{"parse", "-config", configPath, "-metrics-listen-address", "127.0.0.1:0"}, &stdout, &stderr)

	assert.Requires(a.NilError(err))
	assert.Requires(a.Buffer(&stdout).ContainsString(`"variables":["alice"]`))
}

func TestRunParseMetricsEndpointServesDuringParse(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath, _ := writeMetricsParseInputs(t, dir)
	address := freeTCPAddress(t)
	source := &blockingMetricsSource{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	original := newDmesgParseSource
	t.Cleanup(func() {
		newDmesgParseSource = original
	})
	newDmesgParseSource = func(parseio.DmesgOptions) (parseio.Source, error) {
		return source, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- run([]string{
			"parse",
			"-source", "dmesg",
			"-model", modelPath,
			"-metrics-listen-address", address,
		}, &stdout, &stderr)
	}()

	select {
	case <-source.started:
	case err := <-errCh:
		t.Fatalf("parse exited before source blocked: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for parse source to start")
	}

	resp, err := http.Get("http://" + address + "/metrics")
	assert.Requires(a.NilError(err))
	body, err := io.ReadAll(resp.Body)
	assert.Requires(a.NilError(err))
	assert.Requires(a.NilError(resp.Body.Close()))
	assert.Requires(a.Number(resp.StatusCode).EqualTo(http.StatusOK))
	assert.Requires(a.String(string(body)).Contains("drain_cluster_build_info"))

	close(source.release)
	select {
	case err := <-errCh:
		assert.Requires(a.NilError(err))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for parse to finish")
	}
}

func TestRunParseMetricsListenAddressReportsBusyAddress(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath, logPath := writeMetricsParseInputs(t, dir)
	busyListener := listenTCP(t, "127.0.0.1:0")
	defer busyListener.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"parse",
		"-filename", logPath,
		"-model", modelPath,
		"-metrics-listen-address", busyListener.Addr().String(),
	}, &stdout, &stderr)

	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("listen metrics endpoint"))
	assert.Requires(a.String(err.Error()).Contains(busyListener.Addr().String()))
}

func writeMetricsParseInputs(t *testing.T, dir string) (string, string) {
	t.Helper()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}

	logPath := filepath.Join(dir, "target.log")
	if err := os.WriteFile(logPath, []byte("user alice\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	return modelPath, logPath
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener := listenTCP(t, "127.0.0.1:0")
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return address
}

func listenTCP(t *testing.T, address string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen %s: %v", address, err)
	}
	return listener
}

type blockingMetricsSource struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingMetricsSource) Info() parseio.SourceInfo {
	return parseio.SourceInfo{Kind: "dmesg", Name: "dmesg", Finite: false}
}

func (s *blockingMetricsSource) Next(ctx context.Context, _ *parseio.SourceRecord) (bool, error) {
	s.once.Do(func() {
		close(s.started)
	})
	select {
	case <-s.release:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *blockingMetricsSource) Ack(context.Context) error {
	return nil
}

func (s *blockingMetricsSource) Close(context.Context) error {
	return nil
}
