package parseio

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/faceair/drain"
)

func TestNewSinkWritesJSONLPartsByBatchSizeAndMaxAge(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	clock := fakeClock{now: time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)}
	sink, err := NewSink(ctx, nil, SinkOptions{
		Format:            parseFormatJSONL,
		Prefix:            outputDir,
		IncludeParameters: true,
		ExcludeSource:     true,
		BatchSize:         2,
		BatchMaxAge:       time.Minute,
		RunID:             "run-1",
		Now:               (&clock).Now,
	})
	if err != nil {
		t.Fatalf("NewSink() error = %v", err)
	}

	id := 7
	first := Output{TemplateID: &id, ModelID: "model", SourceKind: "file", SourceName: "app.log", Variables: []string{"alice"}, Parameters: []drain.ExtractedParameter{{Value: "alice", MaskName: "user"}}}
	if err := sink.Write(ctx, first); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	if err := sink.Write(ctx, Output{ModelID: "model", Variables: []string{"bob"}}); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	firstPart := readJSONLPart(t, outputDir, "run-1", 0)
	secondPart := readJSONLPart(t, outputDir, "run-1", 1)
	if len(firstPart) != 1 || len(secondPart) != 1 {
		t.Fatalf("part row counts = %d, %d; want 1, 1", len(firstPart), len(secondPart))
	}
	if _, ok := firstPart[0]["source_kind"]; ok {
		t.Fatalf("source_kind was written despite ExcludeSource: %#v", firstPart[0])
	}
	parameters, ok := firstPart[0]["parameters"].([]any)
	if !ok || len(parameters) != 1 {
		t.Fatalf("parameters = %#v, want one parameter", firstPart[0]["parameters"])
	}
}

func TestNewSinkRejectsUnsupportedOutputScheme(t *testing.T) {
	_, err := NewSink(context.Background(), nil, SinkOptions{Format: parseFormatJSONL, Prefix: "ftp://example/out", BatchSize: 1, BatchMaxAge: time.Second})
	if err == nil || !strings.Contains(err.Error(), "unsupported output prefix scheme") {
		t.Fatalf("NewSink() error = %v, want unsupported scheme", err)
	}
}

func TestNewSinkRequiresOutputPrefixForParquet(t *testing.T) {
	_, err := NewSink(context.Background(), nil, SinkOptions{Format: parseFormatParquet, BatchSize: 1, BatchMaxAge: time.Second})
	if err == nil || !strings.Contains(err.Error(), "parquet output requires -output") {
		t.Fatalf("NewSink() error = %v, want parquet output prefix error", err)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func readJSONLPart(t *testing.T, root, runID string, part int) []map[string]any {
	t.Helper()
	path := filepath.Join(root, "format=jsonl", "run_id="+runID, fmt.Sprintf("part-%05d.jsonl", part))
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read part %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode row %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestNewSinkWritesParquetPart(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	sink, err := NewSink(ctx, nil, SinkOptions{
		Format:            parseFormatParquet,
		Prefix:            outputDir,
		IncludeParameters: true,
		BatchSize:         1,
		BatchMaxAge:       time.Minute,
		RunID:             "run-parquet",
		Now:               func() time.Time { return time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewSink() error = %v", err)
	}
	id := 9
	if err := sink.Write(ctx, Output{TemplateID: &id, ModelID: "model", SourceKind: "file", SourceName: "app.log", Variables: []string{"alice"}, Parameters: []drain.ExtractedParameter{{Value: "alice", MaskName: "user"}}}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	path := filepath.Join(outputDir, "format=parquet", "run_id=run-parquet", "part-00000.parquet")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat parquet part: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("parquet part %s is empty", path)
	}
}
