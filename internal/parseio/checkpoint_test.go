package parseio

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSourceCheckpointMissingFileStartsEmpty(t *testing.T) {
	checkpoint, err := LoadSourceCheckpoint(filepath.Join(t.TempDir(), "missing", "checkpoint.json"))
	if err != nil {
		t.Fatalf("LoadSourceCheckpoint() error = %v", err)
	}
	if checkpoint.Kind != "" || checkpoint.Name != "" || checkpoint.Locator != nil || checkpoint.File != nil || checkpoint.Dmesg != nil || checkpoint.Systemd != nil || !checkpoint.UpdatedAt.IsZero() {
		t.Fatalf("LoadSourceCheckpoint() = %#v, want empty checkpoint", checkpoint)
	}
}

func TestSaveAndLoadSourceCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "checkpoint.json")
	want := SourceCheckpoint{
		Kind:    "file",
		Name:    "app.log",
		Locator: map[string]string{"line": "7", "byte": "42"},
		File:    &FileCheckpoint{ByteOffset: 42, LineNumber: 7},
	}

	if err := SaveSourceCheckpoint(context.Background(), path, want); err != nil {
		t.Fatalf("SaveSourceCheckpoint() error = %v", err)
	}
	got, err := LoadSourceCheckpoint(path)
	if err != nil {
		t.Fatalf("LoadSourceCheckpoint() error = %v", err)
	}

	if got.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt was not set")
	}
	got.UpdatedAt = want.UpdatedAt
	if got.Kind != want.Kind || got.Name != want.Name || got.File == nil || *got.File != *want.File || got.Locator["line"] != "7" || got.Locator["byte"] != "42" {
		t.Fatalf("checkpoint round trip = %#v, want %#v", got, want)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary checkpoint files = %v, err = %v; want none", matches, err)
	}
}

func TestSaveSourceCheckpointHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	path := filepath.Join(t.TempDir(), "checkpoint.json")
	err := SaveSourceCheckpoint(ctx, path, SourceCheckpoint{Kind: "file"})
	if err == nil {
		t.Fatal("SaveSourceCheckpoint() error = nil, want canceled context error")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("checkpoint file exists after canceled save: %v", statErr)
	}
}

func TestLoadSourceCheckpointRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "unknown field", content: `{"unknown":true}`, want: "unknown field"},
		{name: "trailing data", content: `{"kind":"file"}\n{"kind":"again"}`, want: "trailing data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}
			_, err := LoadSourceCheckpoint(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadSourceCheckpoint() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
