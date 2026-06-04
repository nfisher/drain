package parseio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
)

type dmesgReaderFactory func(context.Context, bool) (io.ReadCloser, error)

// DmesgSource reads newline-delimited records from the kernel message buffer.
type DmesgSource struct {
	follow        bool
	readerFactory dmesgReaderFactory
	info          SourceInfo

	started bool
	ctx     context.Context
	input   io.ReadCloser
	reader  *bufio.Reader
	pending error

	lineNumber int64
	byteOffset int64
}

// NewDmesgSource creates a source that reads kernel messages directly. When
// follow is true, only new kernel messages are streamed until the source context
// is canceled.
func NewDmesgSource(follow bool) (*DmesgSource, error) {
	return newDmesgSourceWithReaderFactory(follow, openDmesgReader)
}

func newDmesgSourceWithReaderFactory(follow bool, readerFactory dmesgReaderFactory) (*DmesgSource, error) {
	if readerFactory == nil {
		return nil, errors.New("dmesg reader factory must not be nil")
	}
	return &DmesgSource{
		follow:        follow,
		readerFactory: readerFactory,
		info: SourceInfo{
			Kind:   "dmesg",
			Name:   "dmesg",
			Finite: !follow,
		},
	}, nil
}

func (s *DmesgSource) Info() SourceInfo {
	return s.info
}

func (s *DmesgSource) Next(ctx context.Context, record *SourceRecord) (bool, error) {
	if s.pending != nil {
		err := s.pending
		s.pending = nil
		return false, err
	}
	if !s.started {
		if err := s.start(ctx); err != nil {
			return false, err
		}
	}

	raw, err := s.reader.ReadString('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) {
			if s.isCanceledFollow() {
				return false, nil
			}
			if raw != "" {
				s.pending = err
				s.populateRecord(raw, record)
				return true, nil
			}
			return false, err
		}
		if raw == "" {
			if s.isCanceledFollow() {
				return false, nil
			}
			return false, nil
		}
	}

	s.populateRecord(raw, record)
	return true, nil
}

func (s *DmesgSource) Ack(context.Context) error {
	return nil
}

func (s *DmesgSource) Close(context.Context) error {
	if s.input == nil {
		return nil
	}
	return s.input.Close()
}

func (s *DmesgSource) isCanceledFollow() bool {
	return s.follow && s.ctx != nil && s.ctx.Err() != nil
}

func (s *DmesgSource) start(ctx context.Context) error {
	input, err := s.readerFactory(ctx, s.follow)
	if err != nil {
		return err
	}
	s.started = true
	s.ctx = ctx
	s.input = input
	s.reader = bufio.NewReader(input)
	return nil
}

func (s *DmesgSource) populateRecord(raw string, record *SourceRecord) {
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
}
