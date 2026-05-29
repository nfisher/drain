package parseio

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	SystemdLineFormatMessage = "message"
	SystemdLineFormatShort   = "short"
	SystemdLineFormatJSON    = "json"

	maxJournalctlStderrBytes = 4096
)

type systemdCommandFactory func(context.Context, string, ...string) *exec.Cmd

// SystemdOptions configures a systemd journal source.
type SystemdOptions struct {
	Follow      bool
	Units       []string
	Identifiers []string
	Priority    string
	Since       string
	Until       string
	Boot        string
	AfterCursor string
	LineFormat  string
}

// SystemdSource reads newline-delimited JSON records from journalctl.
type SystemdSource struct {
	options        SystemdOptions
	commandFactory systemdCommandFactory
	args           []string
	info           SourceInfo
	lineFormat     string

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
}

// NewSystemdSource creates a source that reads systemd journal entries through
// journalctl. When Follow is true, historical entries are read before new
// entries are streamed until the source context is canceled.
func NewSystemdSource(options SystemdOptions) (*SystemdSource, error) {
	return newSystemdSourceWithCommandFactory(options, exec.CommandContext)
}

func newSystemdSourceWithCommandFactory(options SystemdOptions, commandFactory systemdCommandFactory) (*SystemdSource, error) {
	if commandFactory == nil {
		return nil, errors.New("systemd command factory must not be nil")
	}
	lineFormat, err := normalizeSystemdLineFormat(options.LineFormat)
	if err != nil {
		return nil, err
	}
	args, err := systemdJournalctlArgs(options)
	if err != nil {
		return nil, err
	}
	return &SystemdSource{
		options:        options,
		commandFactory: commandFactory,
		args:           args,
		info: SourceInfo{
			Kind:   "systemd",
			Name:   systemdSourceName(options),
			Finite: !options.Follow,
		},
		lineFormat: lineFormat,
	}, nil
}

func systemdJournalctlArgs(options SystemdOptions) ([]string, error) {
	if err := ValidateSystemdOptions(options); err != nil {
		return nil, err
	}
	args := []string{"--output=json", "--no-pager", "--all"}
	for _, unit := range options.Units {
		if unit != "" {
			args = append(args, "--unit="+unit)
		}
	}
	for _, identifier := range options.Identifiers {
		if identifier != "" {
			args = append(args, "--identifier="+identifier)
		}
	}
	if options.Priority != "" {
		args = append(args, "--priority="+options.Priority)
	}
	if options.Since != "" {
		args = append(args, "--since="+options.Since)
	}
	if options.Until != "" {
		args = append(args, "--until="+options.Until)
	}
	if options.Boot != "" {
		args = append(args, "--boot="+options.Boot)
	}
	if options.AfterCursor != "" {
		args = append(args, "--after-cursor="+options.AfterCursor)
	}
	if options.Follow {
		args = append(args, "--follow", "--no-tail")
	}
	return args, nil
}

// ValidateSystemdOptions validates systemd source options without starting journalctl.
func ValidateSystemdOptions(options SystemdOptions) error {
	_, err := normalizeSystemdLineFormat(options.LineFormat)
	return err
}

func normalizeSystemdLineFormat(format string) (string, error) {
	if format == "" {
		return SystemdLineFormatMessage, nil
	}
	switch format {
	case SystemdLineFormatMessage, SystemdLineFormatShort, SystemdLineFormatJSON:
		return format, nil
	default:
		return "", fmt.Errorf("systemd line format must be message, short, or json, got %q", format)
	}
}

func systemdSourceName(options SystemdOptions) string {
	parts := []string{"journalctl"}
	for _, unit := range options.Units {
		parts = append(parts, "unit="+unit)
	}
	for _, identifier := range options.Identifiers {
		parts = append(parts, "identifier="+identifier)
	}
	if options.Priority != "" {
		parts = append(parts, "priority="+options.Priority)
	}
	if options.Since != "" {
		parts = append(parts, "since="+options.Since)
	}
	if options.Until != "" {
		parts = append(parts, "until="+options.Until)
	}
	if options.Boot != "" {
		parts = append(parts, "boot="+options.Boot)
	}
	if options.AfterCursor != "" {
		parts = append(parts, "after_cursor="+options.AfterCursor)
	}
	if options.Follow {
		parts = append(parts, "follow=true")
	}
	return strings.Join(parts, " ")
}

func (s *SystemdSource) Info() SourceInfo {
	return s.info
}

func (s *SystemdSource) Next(ctx context.Context, record *SourceRecord) (bool, error) {
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
				return s.populateRecord(raw, record)
			}
			if raw != "" {
				s.pending = err
				return s.populateRecord(raw, record)
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

	return s.populateRecord(raw, record)
}

func (s *SystemdSource) Ack(context.Context) error {
	return nil
}

func (s *SystemdSource) Close(context.Context) error {
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

func (s *SystemdSource) start(ctx context.Context) error {
	s.cmdCtx, s.cancelCmd = context.WithCancel(ctx)
	cmd := s.commandFactory(s.cmdCtx, "journalctl", s.args...)
	stderr := &boundedBuffer{limit: maxJournalctlStderrBytes}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.cancelCmd()
		return err
	}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		s.cancelCmd()
		return fmt.Errorf("start journalctl: %w", err)
	}
	s.started = true
	s.cmd = cmd
	s.stdout = stdout
	s.reader = bufio.NewReader(stdout)
	s.stderr = stderr
	return nil
}

func (s *SystemdSource) wait() error {
	if s.waited {
		return s.waitErr
	}
	s.waitErr = s.cmd.Wait()
	s.waited = true
	return s.waitErr
}

func (s *SystemdSource) commandExitError(err error) error {
	if err == nil {
		return nil
	}
	message := fmt.Sprintf("journalctl exited: %v", err)
	if s.stderr != nil {
		stderr := strings.TrimSpace(s.stderr.String())
		if stderr != "" {
			message += fmt.Sprintf(": stderr=%q", stderr)
		}
	}
	return errors.New(message)
}

func (s *SystemdSource) isCanceledFollow() bool {
	return s.options.Follow && s.cmdCtx != nil && s.cmdCtx.Err() != nil
}

func (s *SystemdSource) populateRecord(raw string, record *SourceRecord) (bool, error) {
	rawLine := strings.TrimSuffix(raw, "\n")
	rawLine = strings.TrimSuffix(rawLine, "\r")

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rawLine), &fields); err != nil {
		return false, fmt.Errorf("parse systemd journal JSON: %w", err)
	}
	line, err := systemdRecordLine(fields, rawLine, s.lineFormat)
	if err != nil {
		return false, err
	}

	*record = SourceRecord{
		Line:    line,
		Bytes:   int64(len(raw)),
		Locator: systemdRecordLocator(fields),
	}
	return true, nil
}

func systemdRecordLine(fields map[string]json.RawMessage, rawLine, lineFormat string) (string, error) {
	switch lineFormat {
	case SystemdLineFormatMessage:
		return systemdFirstField(fields, "MESSAGE"), nil
	case SystemdLineFormatShort:
		return systemdShortLine(fields), nil
	case SystemdLineFormatJSON:
		return rawLine, nil
	default:
		return "", fmt.Errorf("systemd line format must be message, short, or json, got %q", lineFormat)
	}
}

func systemdRecordLocator(fields map[string]json.RawMessage) map[string]string {
	locator := make(map[string]string, 8)
	add := func(key, field string) {
		if value := systemdFirstField(fields, field); value != "" {
			locator[key] = value
		}
	}
	add("cursor", "__CURSOR")
	add("realtime_usec", "__REALTIME_TIMESTAMP")
	add("monotonic_usec", "__MONOTONIC_TIMESTAMP")
	add("boot_id", "_BOOT_ID")
	add("unit", "_SYSTEMD_UNIT")
	add("identifier", "SYSLOG_IDENTIFIER")
	add("pid", "_PID")
	add("priority", "PRIORITY")
	if len(locator) == 0 {
		return nil
	}
	return locator
}

func systemdShortLine(fields map[string]json.RawMessage) string {
	var parts []string
	if timestamp := systemdShortTimestamp(systemdFirstField(fields, "__REALTIME_TIMESTAMP")); timestamp != "" {
		parts = append(parts, timestamp)
	}
	if hostname := systemdFirstField(fields, "_HOSTNAME"); hostname != "" {
		parts = append(parts, hostname)
	}

	prefix := strings.Join(parts, " ")
	process := systemdFirstField(fields, "SYSLOG_IDENTIFIER")
	if process == "" {
		process = systemdFirstField(fields, "_COMM")
	}
	if process == "" {
		process = systemdFirstField(fields, "_SYSTEMD_UNIT")
	}
	if process != "" {
		if pid := systemdFirstField(fields, "_PID"); pid != "" {
			process += "[" + pid + "]"
		}
		if prefix != "" {
			prefix += " "
		}
		prefix += process + ":"
	}

	message := systemdFirstField(fields, "MESSAGE")
	if prefix == "" {
		return message
	}
	if message == "" {
		return prefix
	}
	return prefix + " " + message
}

func systemdShortTimestamp(value string) string {
	if value == "" {
		return ""
	}
	usec, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value
	}
	sec := usec / 1_000_000
	nsec := (usec % 1_000_000) * 1_000
	return time.Unix(sec, nsec).Local().Format(time.RFC3339Nano)
}

func systemdFirstField(fields map[string]json.RawMessage, name string) string {
	raw, ok := fields[name]
	if !ok {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil && len(values) > 0 {
		return values[0]
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		return number.String()
	}
	return ""
}
