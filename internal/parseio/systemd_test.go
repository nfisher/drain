package parseio

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	a "github.com/gogunit/gunit/hammy"
)

func TestSystemdSourceReadsMessageAndReportsInfo(t *testing.T) {
	assert := a.New(t)
	skipped := systemdTestEntry("skipped", "s=0", 1700000000000000)
	entry := systemdTestEntry("unit started", "s=1", 1700000000000001)
	journal := &fakeSystemdJournal{
		bootID:  "current-boot",
		entries: []systemdJournalEntry{skipped, entry},
	}
	source, err := newSystemdSourceWithJournalFactory(parseioSystemdTestOptions(), fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))
	defer func() {
		assert.Requires(a.NilError(source.Close(context.Background())))
	}()

	info := source.Info()
	assert.Requires(a.String(info.Kind).EqualTo("systemd"))
	assert.Requires(a.String(info.Name).Contains("unit=demo.service"))
	assert.Requires(a.String(info.Name).Contains("identifier=demo"))
	assert.Requires(a.True(info.Finite))
	assert.Requires(a.Assert(info.SizeBytes == nil, "systemd source should not report size bytes"))
	assert.Requires(a.Number(journal.starts).EqualTo(0))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(ok))
	assert.Requires(a.Number(journal.starts).EqualTo(1))
	assert.Requires(a.Slice(journal.matches).EqualTo(
		"_SYSTEMD_UNIT=demo.service",
		"SYSLOG_IDENTIFIER=demo",
		"PRIORITY=0",
		"PRIORITY=1",
		"PRIORITY=2",
		"PRIORITY=3",
		"PRIORITY=4",
		"_BOOT_ID=current-boot",
	))
	assert.Requires(a.String(journal.seekCursor).EqualTo("s=0"))
	assert.Requires(a.False(journal.seekHead))
	assert.Requires(a.Assert(journal.seekRealtimeUsec == nil, "after-cursor should take precedence over since"))
	assert.Requires(a.Number(journal.nextCalls).EqualTo(2))
	assert.Requires(a.String(record.Line).EqualTo("unit started"))
	rawLine, err := systemdEntryJSON(systemdEntryFields(entry))
	assert.Requires(a.NilError(err))
	assert.Requires(a.Number(record.Bytes).EqualTo(int64(len(rawLine) + 1)))
	assert.Requires(a.String(record.Locator["cursor"]).EqualTo("s=1"))
	assert.Requires(a.String(record.Locator["realtime_usec"]).EqualTo("1700000000000001"))
	assert.Requires(a.String(record.Locator["monotonic_usec"]).EqualTo("99"))
	assert.Requires(a.String(record.Locator["boot_id"]).EqualTo("boot"))
	assert.Requires(a.String(record.Locator["unit"]).EqualTo("demo.service"))
	assert.Requires(a.String(record.Locator["identifier"]).EqualTo("demo"))
	assert.Requires(a.String(record.Locator["pid"]).EqualTo("42"))
	assert.Requires(a.String(record.Locator["priority"]).EqualTo("6"))

	ok, err = source.Next(context.Background(), &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
}

func TestSystemdSourceLineFormats(t *testing.T) {
	assert := a.New(t)
	fields := map[string]string{
		"MESSAGE":              "hello",
		"__REALTIME_TIMESTAMP": "1700000000000000",
		"_HOSTNAME":            "host",
		"SYSLOG_IDENTIFIER":    "demo",
		"_PID":                 "42",
	}
	rawLine, err := systemdEntryJSON(fields)
	assert.Requires(a.NilError(err))

	line, err := systemdRecordLine(fields, rawLine, SystemdLineFormatMessage)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(line).EqualTo("hello"))

	line, err = systemdRecordLine(fields, rawLine, SystemdLineFormatJSON)
	assert.Requires(a.NilError(err))
	var got map[string]string
	assert.Requires(a.NilError(json.Unmarshal([]byte(line), &got)))
	assert.Requires(a.String(got["MESSAGE"]).EqualTo("hello"))
	assert.Requires(a.String(got["__REALTIME_TIMESTAMP"]).EqualTo("1700000000000000"))

	line, err = systemdRecordLine(fields, rawLine, SystemdLineFormatShort)
	assert.Requires(a.NilError(err))
	timestamp := time.Unix(1700000000, 0).Local().Format(time.RFC3339Nano)
	assert.Requires(a.String(line).EqualTo(timestamp + " host demo[42]: hello"))
}

func TestSystemdSourceRejectsInvalidLineFormat(t *testing.T) {
	assert := a.New(t)
	_, err := newSystemdSourceWithJournalFactory(SystemdOptions{LineFormat: "bad"}, fakeSystemdJournalFactory(&fakeSystemdJournal{}))
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).EqualTo(`systemd line format must be message, short, or json, got "bad"`))
}

func TestSystemdSourceReturnsJournalOpenErrors(t *testing.T) {
	assert := a.New(t)
	openErr := errors.New("permission denied")
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{}, func() (systemdJournal, error) {
		return nil, openErr
	})
	assert.Requires(a.NilError(err))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.True(errors.Is(err, openErr)))
}

func TestSystemdSourceReturnsJournalConfigurationErrors(t *testing.T) {
	assert := a.New(t)
	addMatchErr := errors.New("bad match")
	journal := &fakeSystemdJournal{addMatchErr: addMatchErr}
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{Units: []string{"demo.service"}}, fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.True(errors.Is(err, addMatchErr)))
	assert.Requires(a.True(journal.closed))
}

func TestSystemdSourceFollowStopsOnContextCancel(t *testing.T) {
	assert := a.New(t)
	journal := &fakeSystemdJournal{
		entries:       []systemdJournalEntry{systemdTestEntry("live line", "s=1", 1700000000000000)},
		waitForCancel: true,
	}
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{Follow: true}, fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))
	defer func() {
		assert.Requires(a.NilError(source.Close(context.Background())))
	}()

	info := source.Info()
	assert.Requires(a.False(info.Finite))

	ctx, cancel := context.WithCancel(context.Background())
	var record SourceRecord
	ok, err := source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(ok))
	assert.Requires(a.String(record.Line).EqualTo("live line"))

	cancel()
	ok, err = source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
	assert.Requires(a.Number(journal.waits).EqualTo(1))
}

func TestSystemdSourceFollowReturnsWaitErrors(t *testing.T) {
	assert := a.New(t)
	waitErr := errors.New("wait failed")
	journal := &fakeSystemdJournal{waitErr: waitErr}
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{Follow: true}, fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.True(errors.Is(err, waitErr)))
}

func TestSystemdSourceStopsAtUntil(t *testing.T) {
	assert := a.New(t)
	journal := &fakeSystemdJournal{
		entries: []systemdJournalEntry{systemdTestEntry("too new", "s=1", 2000001)},
	}
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{Until: "2"}, fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
}

func TestSystemdPriorityMatches(t *testing.T) {
	assert := a.New(t)
	matches, err := systemdPriorityMatches("warning")
	assert.Requires(a.NilError(err))
	assert.Requires(a.Slice(matches).EqualTo("PRIORITY=0", "PRIORITY=1", "PRIORITY=2", "PRIORITY=3", "PRIORITY=4"))

	matches, err = systemdPriorityMatches("err..notice")
	assert.Requires(a.NilError(err))
	assert.Requires(a.Slice(matches).EqualTo("PRIORITY=3", "PRIORITY=4", "PRIORITY=5"))

	_, err = systemdPriorityMatches("notice..err")
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("low..high"))

	_, err = systemdPriorityMatches("verbose")
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("systemd priority must be"))
}

func TestParseSystemdJournalTime(t *testing.T) {
	assert := a.New(t)
	now := time.Date(2026, 6, 4, 15, 30, 0, 123000000, time.Local)

	usec, err := parseSystemdJournalTime("now", now)
	assert.Requires(a.NilError(err))
	wantNow, err := timeToSystemdUsec(now)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Assert(usec == wantNow, "now should use the supplied timestamp"))

	usec, err = parseSystemdJournalTime("today", now)
	assert.Requires(a.NilError(err))
	wantToday, err := timeToSystemdUsec(time.Date(2026, 6, 4, 0, 0, 0, 0, time.Local))
	assert.Requires(a.NilError(err))
	assert.Requires(a.Assert(usec == wantToday, "today should resolve to local midnight"))

	usec, err = parseSystemdJournalTime("1700000000", now)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Assert(usec == 1700000000000000, "Unix seconds should convert to microseconds"))

	usec, err = parseSystemdJournalTime("1700000000123456", now)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Assert(usec == 1700000000123456, "Unix microseconds should be preserved"))

	usec, err = parseSystemdJournalTime("2026-06-04T12:30:00Z", now)
	assert.Requires(a.NilError(err))
	wantRFC3339, err := timeToSystemdUsec(time.Date(2026, 6, 4, 12, 30, 0, 0, time.UTC))
	assert.Requires(a.NilError(err))
	assert.Requires(a.Assert(usec == wantRFC3339, "RFC3339 timestamp should parse exactly"))

	_, err = parseSystemdJournalTime("1 hour ago", now)
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("not supported"))
}

func TestSystemdSourceRejectsUnsupportedBootDescriptors(t *testing.T) {
	assert := a.New(t)
	_, err := newSystemdSourceWithJournalFactory(SystemdOptions{Boot: "-1"}, fakeSystemdJournalFactory(&fakeSystemdJournal{}))
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).EqualTo(`systemd boot must be 0 or a boot ID, got "-1"`))

	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{Boot: "0123456789abcdef0123456789abcdef"}, fakeSystemdJournalFactory(&fakeSystemdJournal{}))
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(source.journalConfig.Boot).EqualTo("0123456789abcdef0123456789abcdef"))
}

func parseioSystemdTestOptions() SystemdOptions {
	return SystemdOptions{
		Units:       []string{"demo.service"},
		Identifiers: []string{"demo"},
		Priority:    "warning",
		Since:       "today",
		Until:       "tomorrow",
		Boot:        "0",
		AfterCursor: "s=0",
	}
}

func systemdTestEntry(message, cursor string, realtimeUsec uint64) systemdJournalEntry {
	return systemdJournalEntry{
		Fields: map[string]string{
			"MESSAGE":           message,
			"_BOOT_ID":          "boot",
			"_SYSTEMD_UNIT":     "demo.service",
			"SYSLOG_IDENTIFIER": "demo",
			"_PID":              "42",
			"PRIORITY":          "6",
		},
		Cursor:             cursor,
		RealtimeTimestamp:  realtimeUsec,
		MonotonicTimestamp: 99,
	}
}

func fakeSystemdJournalFactory(journal *fakeSystemdJournal) systemdJournalFactory {
	return func() (systemdJournal, error) {
		journal.starts++
		return journal, nil
	}
}

type fakeSystemdJournal struct {
	starts           int
	matches          []string
	bootID           string
	seekHead         bool
	seekRealtimeUsec *uint64
	seekCursor       string
	nextCalls        int
	entries          []systemdJournalEntry
	index            int
	waits            int
	waitForCancel    bool
	closed           bool

	addMatchErr error
	waitErr     error
}

func (j *fakeSystemdJournal) Close() error {
	j.closed = true
	return nil
}

func (j *fakeSystemdJournal) AddMatch(match string) error {
	j.matches = append(j.matches, match)
	return j.addMatchErr
}

func (j *fakeSystemdJournal) GetBootID() (string, error) {
	if j.bootID == "" {
		return "boot", nil
	}
	return j.bootID, nil
}

func (j *fakeSystemdJournal) SeekHead() error {
	j.seekHead = true
	return nil
}

func (j *fakeSystemdJournal) SeekRealtimeUsec(usec uint64) error {
	j.seekRealtimeUsec = &usec
	return nil
}

func (j *fakeSystemdJournal) SeekCursor(cursor string) error {
	j.seekCursor = cursor
	return nil
}

func (j *fakeSystemdJournal) Next() (bool, error) {
	j.nextCalls++
	if j.index >= len(j.entries) {
		return false, nil
	}
	j.index++
	return true, nil
}

func (j *fakeSystemdJournal) Entry() (systemdJournalEntry, error) {
	if j.index == 0 || j.index > len(j.entries) {
		return systemdJournalEntry{}, errors.New("journal is not positioned on an entry")
	}
	return j.entries[j.index-1], nil
}

func (j *fakeSystemdJournal) Wait(ctx context.Context) error {
	j.waits++
	if j.waitForCancel {
		<-ctx.Done()
		return ctx.Err()
	}
	if j.waitErr != nil {
		return j.waitErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("fake journal wait would block")
}

func TestSystemdEntryJSONIncludesAddressFields(t *testing.T) {
	assert := a.New(t)
	entry := systemdTestEntry("hello", "s=1", 1700000000000000)
	rawLine, err := systemdEntryJSON(systemdEntryFields(entry))
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(strings.Contains(rawLine, `"MESSAGE":"hello"`)))
	assert.Requires(a.True(strings.Contains(rawLine, `"__CURSOR":"s=1"`)))
	assert.Requires(a.True(strings.Contains(rawLine, `"__REALTIME_TIMESTAMP":"1700000000000000"`)))
	assert.Requires(a.True(strings.Contains(rawLine, `"__MONOTONIC_TIMESTAMP":"99"`)))
}

func TestSystemdSourceCheckpointResumesAfterCursor(t *testing.T) {
	assert := a.New(t)
	journal := &fakeSystemdJournal{entries: []systemdJournalEntry{
		systemdTestEntry("skipped", "s=1", 1700000000000001),
		systemdTestEntry("next", "s=2", 1700000000000002),
	}}
	source, err := newSystemdSourceWithJournalFactory(SystemdOptions{}, fakeSystemdJournalFactory(journal))
	assert.Requires(a.NilError(err))
	defer func() { assert.Requires(a.NilError(source.Close(context.Background()))) }()

	assert.Requires(a.NilError(source.Resume(SourceCheckpoint{Systemd: &SystemdCheckpoint{Cursor: "s=1"}})))
	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(ok))
	assert.Requires(a.String(journal.seekCursor).EqualTo("s=1"))
	assert.Requires(a.Number(journal.nextCalls).EqualTo(2))
	assert.Requires(a.String(record.Line).EqualTo("next"))

	checkpoint := source.Checkpoint()
	assert.Requires(a.String(checkpoint.Systemd.Cursor).EqualTo("s=2"))
}
