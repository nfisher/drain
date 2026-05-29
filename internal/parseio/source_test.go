package parseio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

func TestFileSourceReadsLinesAndReportsInfo(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "target.log")
	content := "first line\nsecond line\r\nthird"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	source, err := NewFileSource(path)
	assert.Requires(a.NilError(err))

	defer func() {
		assert.Requires(a.NilError(source.Close(context.Background())))
	}()

	info := source.Info()
	assert.Requires(a.String(info.Kind).EqualTo("file"))
	assert.Requires(a.String(info.Name).EqualTo(path))

	assert.Requires(a.True(info.Finite))
	assert.Requires(a.Assert(!(info.SizeBytes == nil || *info.SizeBytes != int64(len(content))), "size mismatch: want %d got %v", len(content), info.SizeBytes))

	var record SourceRecord
	var lines []string
	var sizes []int64
	var lineNumbers []int64
	var byteOffsets []int64
	var locators []map[string]string
	for {
		ok, err := source.Next(context.Background(), &record)
		assert.Requires(a.NilError(err))

		if !ok {
			break
		}
		lines = append(lines, record.Line)
		sizes = append(sizes, record.Bytes)
		lineNumbers = append(lineNumbers, record.LineNumber)
		byteOffsets = append(byteOffsets, record.ByteOffset)
		locators = append(locators, cloneStringMap(record.Locator))

		assert.Requires(a.NilError(source.Ack(context.Background())))
	}
	assert.Requires(a.Slice(lines).EqualTo("first line", "second line", "third"))
	assert.Requires(a.Slice(sizes).EqualTo(11, 13, 5))
	assert.Requires(a.Slice(lineNumbers).EqualTo(1, 2, 3))
	assert.Requires(a.Slice(byteOffsets).EqualTo(0, 11, 24))
	assert.Requires(a.Slice(locators).EqualTo(
		map[string]string{"line": "1", "byte": "0"},
		map[string]string{"line": "2", "byte": "11"},
		map[string]string{"line": "3", "byte": "24"},
	))
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
