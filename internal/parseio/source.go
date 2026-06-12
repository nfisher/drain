package parseio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

// Source produces log records for the parse pipeline.
type Source interface {
	Info() SourceInfo
	Next(ctx context.Context, record *SourceRecord) (bool, error)
	Ack(ctx context.Context) error
	Close(ctx context.Context) error
}

// SourceRecord is one log line read from a source.
type SourceRecord struct {
	Line       string
	Bytes      int64
	LineNumber int64
	ByteOffset int64
	Locator    map[string]string
}

// SourceInfo describes a source for tracing and operational reporting.
type SourceInfo struct {
	Kind      string
	Name      string
	Finite    bool
	SizeBytes *int64
}

// FileSource reads newline-delimited records from a local file.
type FileSource struct {
	file       *os.File
	reader     *bufio.Reader
	info       SourceInfo
	lineNumber int64
	byteOffset int64
}

// NewFileSource opens path as a finite local-file source.
func NewFileSource(path string) (*FileSource, error) {
	file, err := os.Open(path) // #nosec G304 -- file source path is an explicit CLI/config input.
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	size := info.Size()
	return &FileSource{
		file:   file,
		reader: bufio.NewReader(file),
		info: SourceInfo{
			Kind:      "file",
			Name:      path,
			Finite:    true,
			SizeBytes: &size,
		},
	}, nil
}

func (s *FileSource) Info() SourceInfo {
	return s.info
}

func (s *FileSource) Next(_ context.Context, record *SourceRecord) (bool, error) {
	raw, err := s.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && raw == "" {
			return false, nil
		}
		if !errors.Is(err, io.EOF) {
			return false, err
		}
	}

	lineNumber := s.lineNumber + 1
	byteOffset := s.byteOffset
	bytesRead := int64(len(raw))
	line := raw
	if strings.HasSuffix(line, "\n") {
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
	}

	record.Line = line
	record.Bytes = bytesRead
	record.LineNumber = lineNumber
	record.ByteOffset = byteOffset
	record.Locator = map[string]string{
		"line": strconv.FormatInt(lineNumber, 10),
		"byte": strconv.FormatInt(byteOffset, 10),
	}
	s.lineNumber = lineNumber
	s.byteOffset += bytesRead
	return true, nil
}

func (s *FileSource) Ack(context.Context) error {
	return nil
}

func (s *FileSource) Close(context.Context) error {
	return s.file.Close()
}
