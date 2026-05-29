package parseio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const maxDmesgStderrBytes = 4096

type dmesgCommandFactory func(context.Context, string, ...string) *exec.Cmd

// DmesgSource reads newline-delimited records from dmesg.
type DmesgSource struct {
	follow         bool
	commandFactory dmesgCommandFactory
	info           SourceInfo

	started   bool
	waited    bool
	cmdCtx    context.Context
	cancelCmd context.CancelFunc
	cmd       *exec.Cmd
	stdout    io.ReadCloser
	reader    *bufio.Reader
	stderr    *boundedBuffer
	waitErr   error
	pending   error

	lineNumber int64
	byteOffset int64
}

// NewDmesgSource creates a source that reads dmesg output. When follow is true,
// dmesg is run in follow mode and reads until the source context is canceled.
func NewDmesgSource(follow bool) (*DmesgSource, error) {
	return newDmesgSourceWithCommandFactory(follow, exec.CommandContext)
}

func newDmesgSourceWithCommandFactory(follow bool, commandFactory dmesgCommandFactory) (*DmesgSource, error) {
	if commandFactory == nil {
		return nil, errors.New("dmesg command factory must not be nil")
	}
	return &DmesgSource{
		follow:         follow,
		commandFactory: commandFactory,
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
			if errors.Is(err, os.ErrClosed) {
				exitErr := s.commandExitError(s.wait())
				if raw == "" {
					return false, exitErr
				}
				if exitErr != nil {
					s.pending = exitErr
				}
				s.populateRecord(raw, record)
				return true, nil
			}
			if raw != "" {
				s.pending = err
				s.populateRecord(raw, record)
				return true, nil
			}
			return false, err
		}
		waitErr := s.wait()
		exitErr := s.commandExitError(waitErr)
		if raw == "" {
			if s.isCanceledFollow() {
				return false, nil
			}
			return false, exitErr
		}
		if exitErr != nil && !s.isCanceledFollow() {
			s.pending = exitErr
		}
	}

	s.populateRecord(raw, record)
	return true, nil
}

func (s *DmesgSource) Ack(context.Context) error {
	return nil
}

func (s *DmesgSource) Close(context.Context) error {
	if s.cancelCmd != nil {
		s.cancelCmd()
	}
	if s.stdout != nil {
		_ = s.stdout.Close()
	}
	if !s.started || s.waited {
		return nil
	}
	if err := s.commandExitError(s.wait()); err != nil && !s.isCanceledFollow() {
		return err
	}
	return nil
}

func (s *DmesgSource) start(ctx context.Context) error {
	args := []string(nil)
	if s.follow {
		args = []string{"-w"}
	}
	s.cmdCtx, s.cancelCmd = context.WithCancel(ctx)
	cmd := s.commandFactory(s.cmdCtx, "dmesg", args...)
	stderr := &boundedBuffer{limit: maxDmesgStderrBytes}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.cancelCmd()
		return err
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		s.cancelCmd()
		return fmt.Errorf("start dmesg: %w", err)
	}
	s.started = true
	s.cmd = cmd
	s.stdout = stdout
	s.reader = bufio.NewReader(stdout)
	s.stderr = stderr
	return nil
}

func (s *DmesgSource) wait() error {
	if s.waited {
		return s.waitErr
	}
	s.waitErr = s.cmd.Wait()
	s.waited = true
	return s.waitErr
}

func (s *DmesgSource) commandExitError(err error) error {
	if err == nil {
		return nil
	}
	message := fmt.Sprintf("dmesg exited: %v", err)
	if s.stderr != nil {
		stderr := strings.TrimSpace(s.stderr.String())
		if stderr != "" {
			message += fmt.Sprintf(": stderr=%q", stderr)
		}
	}
	return errors.New(message)
}

func (s *DmesgSource) isCanceledFollow() bool {
	return s.follow && s.cmdCtx != nil && s.cmdCtx.Err() != nil
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

type boundedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if b.limit <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return written, nil
	}
	remaining := b.limit - b.buffer.Len()
	if remaining <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return written, nil
	}
	if len(p) > remaining {
		_, _ = b.buffer.Write(p[:remaining])
		b.truncated = true
		return written, nil
	}
	_, _ = b.buffer.Write(p)
	return written, nil
}

func (b *boundedBuffer) String() string {
	value := b.buffer.String()
	if b.truncated {
		return value + "...(truncated)"
	}
	return value
}
