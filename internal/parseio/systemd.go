package parseio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SystemdLineFormatMessage = "message"
	SystemdLineFormatShort   = "short"
	SystemdLineFormatJSON    = "json"

	systemdJournalUsecPerSec = 1000 * 1000
	maxSystemdJournalSec     = int64(^uint64(0) / systemdJournalUsecPerSec)
)

type systemdJournalFactory func() (systemdJournal, error)

type systemdJournal interface {
	Close() error
	AddMatch(string) error
	GetBootID() (string, error)
	SeekHead() error
	SeekRealtimeUsec(uint64) error
	SeekCursor(string) error
	Next() (bool, error)
	Entry() (systemdJournalEntry, error)
	Wait(context.Context) error
}

type systemdJournalEntry struct {
	Fields             map[string]string
	Cursor             string
	RealtimeTimestamp  uint64
	MonotonicTimestamp uint64
}

type systemdJournalConfig struct {
	Matches     []string
	SinceUsec   *uint64
	UntilUsec   *uint64
	Boot        string
	AfterCursor string
}

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

// SystemdSource reads entries from the systemd journal directly.
type SystemdSource struct {
	options        SystemdOptions
	journalFactory systemdJournalFactory
	journalConfig  systemdJournalConfig
	info           SourceInfo
	lineFormat     string

	started    bool
	journal    systemdJournal
	lastCursor string
}

// NewSystemdSource creates a source that reads systemd journal entries directly.
// When Follow is true, historical entries are read before new entries are
// streamed until the source context is canceled.
func NewSystemdSource(options SystemdOptions) (*SystemdSource, error) {
	return newSystemdSourceWithJournalFactory(options, openSystemdJournal)
}

func newSystemdSourceWithJournalFactory(options SystemdOptions, journalFactory systemdJournalFactory) (*SystemdSource, error) {
	if journalFactory == nil {
		return nil, errors.New("systemd journal factory must not be nil")
	}
	lineFormat, err := normalizeSystemdLineFormat(options.LineFormat)
	if err != nil {
		return nil, err
	}
	journalConfig, err := newSystemdJournalConfig(options, time.Now())
	if err != nil {
		return nil, err
	}
	return &SystemdSource{
		options:        options,
		journalFactory: journalFactory,
		journalConfig:  journalConfig,
		info: SourceInfo{
			Kind:   "systemd",
			Name:   systemdSourceName(options),
			Finite: !options.Follow,
		},
		lineFormat: lineFormat,
	}, nil
}

// ValidateSystemdOptions validates systemd source options without opening the
// journal.
func ValidateSystemdOptions(options SystemdOptions) error {
	if _, err := normalizeSystemdLineFormat(options.LineFormat); err != nil {
		return err
	}
	_, err := newSystemdJournalConfig(options, time.Now())
	return err
}

func newSystemdJournalConfig(options SystemdOptions, now time.Time) (systemdJournalConfig, error) {
	var config systemdJournalConfig
	for _, unit := range options.Units {
		if unit != "" {
			config.Matches = append(config.Matches, "_SYSTEMD_UNIT="+unit)
		}
	}
	for _, identifier := range options.Identifiers {
		if identifier != "" {
			config.Matches = append(config.Matches, "SYSLOG_IDENTIFIER="+identifier)
		}
	}
	priorityMatches, err := systemdPriorityMatches(options.Priority)
	if err != nil {
		return systemdJournalConfig{}, err
	}
	config.Matches = append(config.Matches, priorityMatches...)
	if options.Since != "" {
		sinceUsec, err := parseSystemdJournalTime(options.Since, now)
		if err != nil {
			return systemdJournalConfig{}, fmt.Errorf("systemd since: %w", err)
		}
		config.SinceUsec = &sinceUsec
	}
	if options.Until != "" {
		untilUsec, err := parseSystemdJournalTime(options.Until, now)
		if err != nil {
			return systemdJournalConfig{}, fmt.Errorf("systemd until: %w", err)
		}
		config.UntilUsec = &untilUsec
	}
	if config.SinceUsec != nil && config.UntilUsec != nil && *config.SinceUsec > *config.UntilUsec {
		return systemdJournalConfig{}, errors.New("systemd since must be before or equal to until")
	}
	if options.Boot != "" {
		boot, err := normalizeSystemdBoot(options.Boot)
		if err != nil {
			return systemdJournalConfig{}, err
		}
		config.Boot = boot
	}
	config.AfterCursor = options.AfterCursor
	return config, nil
}

func systemdPriorityMatches(priority string) ([]string, error) {
	priority = strings.TrimSpace(priority)
	if priority == "" {
		return nil, nil
	}
	if lowText, highText, ok := strings.Cut(priority, ".."); ok {
		low, err := systemdPriorityLevel(lowText)
		if err != nil {
			return nil, err
		}
		high, err := systemdPriorityLevel(highText)
		if err != nil {
			return nil, err
		}
		if low > high {
			return nil, fmt.Errorf("systemd priority range must be low..high, got %q", priority)
		}
		return systemdPriorityRangeMatches(low, high), nil
	}
	level, err := systemdPriorityLevel(priority)
	if err != nil {
		return nil, err
	}
	return systemdPriorityRangeMatches(0, level), nil
}

func systemdPriorityRangeMatches(low, high int) []string {
	matches := make([]string, 0, high-low+1)
	for level := low; level <= high; level++ {
		matches = append(matches, "PRIORITY="+strconv.Itoa(level))
	}
	return matches
}

func systemdPriorityLevel(priority string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(priority)) {
	case "0", "emerg", "emergency", "panic":
		return 0, nil
	case "1", "alert":
		return 1, nil
	case "2", "crit", "critical":
		return 2, nil
	case "3", "err", "error":
		return 3, nil
	case "4", "warning", "warn":
		return 4, nil
	case "5", "notice":
		return 5, nil
	case "6", "info", "informational":
		return 6, nil
	case "7", "debug":
		return 7, nil
	default:
		return 0, fmt.Errorf("systemd priority must be 0..7, a priority name, or a range, got %q", priority)
	}
}

func parseSystemdJournalTime(value string, now time.Time) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("time must not be empty")
	}
	localNow := now.In(time.Local)
	today := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, time.Local)
	switch strings.ToLower(value) {
	case "now":
		return timeToSystemdUsec(now)
	case "today":
		return timeToSystemdUsec(today)
	case "yesterday":
		return timeToSystemdUsec(today.AddDate(0, 0, -1))
	case "tomorrow":
		return timeToSystemdUsec(today.AddDate(0, 0, 1))
	}
	if isDecimalDigits(value) {
		usec, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse Unix timestamp %q: %w", value, err)
		}
		if len(value) <= 12 {
			usec *= systemdJournalUsecPerSec
		}
		return usec, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return timeToSystemdUsec(parsed)
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	} {
		if parsed, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return timeToSystemdUsec(parsed)
		}
	}
	return 0, fmt.Errorf("time %q is not supported; use RFC3339, Unix seconds/usec, now, today, yesterday, or tomorrow", value)
}

func timeToSystemdUsec(value time.Time) (uint64, error) {
	sec := value.Unix()
	if sec < 0 {
		return 0, fmt.Errorf("time %q is before the Unix epoch", value.Format(time.RFC3339Nano))
	}
	if sec > maxSystemdJournalSec {
		return 0, fmt.Errorf("time %q is too large for systemd journal timestamps", value.Format(time.RFC3339Nano))
	}
	return uint64(sec)*systemdJournalUsecPerSec + uint64(value.Nanosecond()/1000), nil // #nosec G115 -- sec is checked non-negative and bounded above.
}

func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func normalizeSystemdBoot(boot string) (string, error) {
	boot = strings.TrimSpace(boot)
	if boot == "0" {
		return boot, nil
	}
	if isSystemdBootID(boot) {
		return boot, nil
	}
	return "", fmt.Errorf("systemd boot must be 0 or a boot ID, got %q", boot)
}

func isSystemdBootID(value string) bool {
	if len(value) == 32 {
		return isHexString(value)
	}
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !isHexRune(r) {
				return false
			}
		}
	}
	return true
}

func isHexString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !isHexRune(r) {
			return false
		}
	}
	return true
}

func isHexRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
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

func (s *SystemdSource) Resume(checkpoint SourceCheckpoint) error {
	if checkpoint.Systemd == nil || checkpoint.Systemd.Cursor == "" {
		return nil
	}
	s.journalConfig.AfterCursor = checkpoint.Systemd.Cursor
	s.options.AfterCursor = checkpoint.Systemd.Cursor
	s.lastCursor = checkpoint.Systemd.Cursor
	s.info.Name = systemdSourceName(s.options)
	return nil
}

func (s *SystemdSource) Checkpoint() SourceCheckpoint {
	locator := map[string]string(nil)
	if s.lastCursor != "" {
		locator = map[string]string{"cursor": s.lastCursor}
	}
	return SourceCheckpoint{
		Kind:    s.info.Kind,
		Name:    s.info.Name,
		Locator: locator,
		Systemd: &SystemdCheckpoint{Cursor: s.lastCursor},
	}
}

func (s *SystemdSource) Next(ctx context.Context, record *SourceRecord) (bool, error) {
	if !s.started {
		if err := s.start(); err != nil {
			return false, err
		}
	}
	for {
		ok, err := s.journal.Next()
		if err != nil {
			return false, err
		}
		if ok {
			entry, err := s.journal.Entry()
			if err != nil {
				return false, err
			}
			if s.journalConfig.UntilUsec != nil && entry.RealtimeTimestamp > *s.journalConfig.UntilUsec {
				return false, nil
			}
			return s.populateRecord(entry, record)
		}
		if !s.options.Follow {
			return false, nil
		}
		if err := s.journal.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			return false, err
		}
	}
}

func (s *SystemdSource) Ack(context.Context) error {
	return nil
}

func (s *SystemdSource) Close(context.Context) error {
	if !s.started || s.journal == nil {
		return nil
	}
	return s.journal.Close()
}

func (s *SystemdSource) start() error {
	journal, err := s.journalFactory()
	if err != nil {
		return err
	}
	if err := s.configureJournal(journal); err != nil {
		_ = journal.Close()
		return err
	}
	s.started = true
	s.journal = journal
	return nil
}

func (s *SystemdSource) configureJournal(journal systemdJournal) error {
	matches := append([]string(nil), s.journalConfig.Matches...)
	if s.journalConfig.Boot != "" {
		bootID := s.journalConfig.Boot
		if bootID == "0" {
			currentBootID, err := journal.GetBootID()
			if err != nil {
				return fmt.Errorf("read current systemd boot ID: %w", err)
			}
			bootID = currentBootID
		}
		matches = append(matches, "_BOOT_ID="+bootID)
	}
	for _, match := range matches {
		if err := journal.AddMatch(match); err != nil {
			return fmt.Errorf("add systemd journal match %q: %w", match, err)
		}
	}
	if s.journalConfig.AfterCursor != "" {
		if err := journal.SeekCursor(s.journalConfig.AfterCursor); err != nil {
			return fmt.Errorf("seek systemd journal cursor %q: %w", s.journalConfig.AfterCursor, err)
		}
		if _, err := journal.Next(); err != nil {
			return fmt.Errorf("skip systemd journal cursor %q: %w", s.journalConfig.AfterCursor, err)
		}
		return nil
	}
	if s.journalConfig.SinceUsec != nil {
		if err := journal.SeekRealtimeUsec(*s.journalConfig.SinceUsec); err != nil {
			return fmt.Errorf("seek systemd journal realtime timestamp: %w", err)
		}
		return nil
	}
	if err := journal.SeekHead(); err != nil {
		return fmt.Errorf("seek systemd journal head: %w", err)
	}
	return nil
}

func (s *SystemdSource) populateRecord(entry systemdJournalEntry, record *SourceRecord) (bool, error) {
	fields := systemdEntryFields(entry)
	rawLine, err := systemdEntryJSON(fields)
	if err != nil {
		return false, err
	}
	line, err := systemdRecordLine(fields, rawLine, s.lineFormat)
	if err != nil {
		return false, err
	}

	locator := systemdRecordLocator(fields)
	*record = SourceRecord{
		Line:    line,
		Bytes:   int64(len(rawLine) + 1),
		Locator: locator,
	}
	s.lastCursor = locator["cursor"]
	return true, nil
}

func systemdEntryFields(entry systemdJournalEntry) map[string]string {
	fields := make(map[string]string, len(entry.Fields)+3)
	for name, value := range entry.Fields {
		fields[name] = value
	}
	if entry.Cursor != "" {
		fields["__CURSOR"] = entry.Cursor
	}
	if entry.RealtimeTimestamp != 0 {
		fields["__REALTIME_TIMESTAMP"] = strconv.FormatUint(entry.RealtimeTimestamp, 10)
	}
	if entry.MonotonicTimestamp != 0 {
		fields["__MONOTONIC_TIMESTAMP"] = strconv.FormatUint(entry.MonotonicTimestamp, 10)
	}
	return fields
}

func systemdEntryJSON(fields map[string]string) (string, error) {
	raw, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("format systemd journal JSON: %w", err)
	}
	return string(raw), nil
}

func systemdRecordLine(fields map[string]string, rawLine, lineFormat string) (string, error) {
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

func systemdRecordLocator(fields map[string]string) map[string]string {
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

func systemdShortLine(fields map[string]string) string {
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
	sec := usec / systemdJournalUsecPerSec
	nsec := (usec % systemdJournalUsecPerSec) * 1_000
	return time.Unix(sec, nsec).Local().Format(time.RFC3339Nano)
}

func systemdFirstField(fields map[string]string, name string) string {
	return fields[name]
}
