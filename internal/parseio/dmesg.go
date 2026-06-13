package parseio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
)

const DefaultDmesgKmsgPath = "/dev/kmsg"

type dmesgReaderFactory func(context.Context, DmesgOptions) (io.ReadCloser, error)

// DmesgOptions configures a direct kernel message buffer source.
type DmesgOptions struct {
	Follow      bool
	KmsgPath    string
	AfterCursor string
}

// DmesgSource reads newline-delimited records from the kernel message buffer.
type DmesgSource struct {
	options       DmesgOptions
	readerFactory dmesgReaderFactory
	info          SourceInfo

	started bool
	ctx     context.Context
	input   io.ReadCloser
	reader  *bufio.Reader
	pending error

	lineNumber int64
	byteOffset int64
	lastCursor string
}

// NewDmesgSource creates a source that reads kernel messages directly. When
// follow is true, only new kernel messages are streamed until the source context
// is canceled.
func NewDmesgSource(follow bool) (*DmesgSource, error) {
	return NewDmesgSourceWithOptions(DmesgOptions{Follow: follow})
}

// NewDmesgSourceWithOptions creates a source that reads kernel messages directly.
func NewDmesgSourceWithOptions(options DmesgOptions) (*DmesgSource, error) {
	return newDmesgSourceWithReaderFactory(options, openDmesgReader)
}

func newDmesgSourceWithReaderFactory(options DmesgOptions, readerFactory dmesgReaderFactory) (*DmesgSource, error) {
	if readerFactory == nil {
		return nil, errors.New("dmesg reader factory must not be nil")
	}
	options = normalizeDmesgOptions(options)
	return &DmesgSource{
		options:       options,
		readerFactory: readerFactory,
		info: SourceInfo{
			Kind:   "dmesg",
			Name:   "dmesg",
			Finite: !options.Follow,
		},
	}, nil
}

func normalizeDmesgOptions(options DmesgOptions) DmesgOptions {
	if strings.TrimSpace(options.KmsgPath) == "" {
		options.KmsgPath = DefaultDmesgKmsgPath
	}
	return options
}

func (s *DmesgSource) Info() SourceInfo {
	return s.info
}

func (s *DmesgSource) Resume(checkpoint SourceCheckpoint) error {
	if checkpoint.Dmesg == nil {
		return nil
	}
	if checkpoint.Dmesg.LineNumber < 0 || checkpoint.Dmesg.ByteOffset < 0 {
		return errors.New("dmesg checkpoint offsets must not be negative")
	}
	s.options.AfterCursor = checkpoint.Dmesg.Cursor
	s.lastCursor = checkpoint.Dmesg.Cursor
	return nil
}

func (s *DmesgSource) Checkpoint() SourceCheckpoint {
	locator := map[string]string{
		"line": strconv.FormatInt(s.lineNumber, 10),
		"byte": strconv.FormatInt(s.byteOffset, 10),
	}
	if s.lastCursor != "" {
		locator["cursor"] = s.lastCursor
	}
	return SourceCheckpoint{
		Kind:    s.info.Kind,
		Name:    s.info.Name,
		Locator: locator,
		Dmesg: &DmesgCheckpoint{
			Cursor:     s.lastCursor,
			ByteOffset: s.byteOffset,
			LineNumber: s.lineNumber,
		},
	}
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

	for {
		s.populateRecord(raw, record)
		if !s.shouldSkipRecord(record.Locator["cursor"]) {
			return true, nil
		}
		raw, err = s.reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				if s.isCanceledFollow() {
					return false, nil
				}
				if raw != "" {
					s.pending = err
					continue
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
	}
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
	return s.options.Follow && s.ctx != nil && s.ctx.Err() != nil
}

func (s *DmesgSource) start(ctx context.Context) error {
	input, err := s.readerFactory(ctx, s.options)
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
	rawLine := raw
	if strings.HasSuffix(rawLine, "\n") {
		rawLine = strings.TrimSuffix(rawLine, "\n")
		rawLine = strings.TrimSuffix(rawLine, "\r")
	}
	line := formatDmesgRecord(rawLine)

	record.Line = line
	record.Bytes = bytesRead
	record.LineNumber = lineNumber
	record.ByteOffset = byteOffset
	cursor := dmesgRecordCursor(raw, byteOffset)
	record.Locator = map[string]string{
		"line":   strconv.FormatInt(lineNumber, 10),
		"byte":   strconv.FormatInt(byteOffset, 10),
		"cursor": cursor,
	}
	s.lineNumber = lineNumber
	s.byteOffset += bytesRead
	s.lastCursor = cursor
}

func dmesgRecordCursor(raw string, byteOffset int64) string {
	metadata, _, ok := strings.Cut(raw, ";")
	if ok {
		fields := strings.Split(metadata, ",")
		if len(fields) >= 2 && strings.TrimSpace(fields[1]) != "" {
			return strings.TrimSpace(fields[1])
		}
	}
	return strconv.FormatInt(byteOffset, 10)
}

func (s *DmesgSource) shouldSkipRecord(cursor string) bool {
	after := s.options.AfterCursor
	if after == "" || cursor == "" {
		return false
	}
	currentSeq, currentErr := strconv.ParseUint(cursor, 10, 64)
	afterSeq, afterErr := strconv.ParseUint(after, 10, 64)
	if currentErr == nil && afterErr == nil {
		if currentSeq > afterSeq {
			s.options.AfterCursor = ""
			return false
		}
		return true
	}
	if cursor == after {
		s.options.AfterCursor = ""
	}
	return true
}
