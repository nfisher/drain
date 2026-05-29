package parseio

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileSourceReadsLinesAndReportsInfo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target.log")
	content := "first line\nsecond line\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	source, err := NewFileSource(path)
	if err != nil {
		t.Fatalf("new file source: %v", err)
	}
	defer func() {
		if err := source.Close(context.Background()); err != nil {
			t.Fatalf("close source: %v", err)
		}
	}()

	info := source.Info()
	if got, want := info.Kind, "file"; got != want {
		t.Fatalf("kind mismatch: want %q got %q", want, got)
	}
	if got, want := info.Name, path; got != want {
		t.Fatalf("name mismatch: want %q got %q", want, got)
	}
	if !info.Finite {
		t.Fatal("file source should be finite")
	}
	if info.SizeBytes == nil || *info.SizeBytes != int64(len(content)) {
		t.Fatalf("size mismatch: want %d got %v", len(content), info.SizeBytes)
	}

	var record SourceRecord
	var lines []string
	var sizes []int64
	for {
		ok, err := source.Next(context.Background(), &record)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			break
		}
		lines = append(lines, record.Line)
		sizes = append(sizes, record.Bytes)
		if err := source.Ack(context.Background()); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if want := []string{"first line", "second line"}; !reflect.DeepEqual(lines, want) {
		t.Fatalf("lines mismatch:\nwant %#v\ngot  %#v", want, lines)
	}
	if want := []int64{10, 11}; !reflect.DeepEqual(sizes, want) {
		t.Fatalf("sizes mismatch:\nwant %#v\ngot  %#v", want, sizes)
	}
}
