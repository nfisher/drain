package parseio

import (
	"bufio"
	"context"
	"os"
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
	Line  string
	Bytes int64
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
	file    *os.File
	scanner *bufio.Scanner
	info    SourceInfo
}

// NewFileSource opens path as a finite local-file source.
func NewFileSource(path string) (*FileSource, error) {
	file, err := os.Open(path)
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
		file:    file,
		scanner: bufio.NewScanner(file),
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
	if !s.scanner.Scan() {
		if err := s.scanner.Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	line := s.scanner.Text()
	record.Line = line
	record.Bytes = int64(len(s.scanner.Bytes()))
	return true, nil
}

func (s *FileSource) Ack(context.Context) error {
	return nil
}

func (s *FileSource) Close(context.Context) error {
	return s.file.Close()
}
