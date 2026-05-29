package parseio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	a "github.com/gogunit/gunit/hammy"
)

func TestSystemdSourceReadsMessageAndReportsInfo(t *testing.T) {
	assert := a.New(t)
	raw := `{"MESSAGE":"unit started","__CURSOR":"s=1","__REALTIME_TIMESTAMP":"1700000000000000","__MONOTONIC_TIMESTAMP":"99","_BOOT_ID":"boot","_SYSTEMD_UNIT":"demo.service","SYSLOG_IDENTIFIER":"demo","_PID":"42","PRIORITY":"6"}` + "\n"
	var command capturedSystemdCommand
	source, err := newSystemdSourceWithCommandFactory(parseioSystemdTestOptions(), systemdHelperCommandFactory(
		raw,
		"",
		0,
		false,
		&command,
	))
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
	assert.Requires(a.String(command.name).EqualTo(""))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(ok))
	assert.Requires(a.String(command.name).EqualTo("journalctl"))
	assert.Requires(a.Slice(command.args).EqualTo(
		"--output=json",
		"--no-pager",
		"--all",
		"--unit=demo.service",
		"--identifier=demo",
		"--priority=warning",
		"--since=today",
		"--until=tomorrow",
		"--boot=0",
		"--after-cursor=s=0",
	))
	assert.Requires(a.String(record.Line).EqualTo("unit started"))
	assert.Requires(a.Number(record.Bytes).EqualTo(int64(len(raw))))
	assert.Requires(a.String(record.Locator["cursor"]).EqualTo("s=1"))
	assert.Requires(a.String(record.Locator["realtime_usec"]).EqualTo("1700000000000000"))
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
	raw := `{"MESSAGE":"hello","__REALTIME_TIMESTAMP":"1700000000000000","_HOSTNAME":"host","SYSLOG_IDENTIFIER":"demo","_PID":"42"}`
	var fields map[string]json.RawMessage
	assert.Requires(a.NilError(json.Unmarshal([]byte(raw), &fields)))

	line, err := systemdRecordLine(fields, raw, SystemdLineFormatMessage)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(line).EqualTo("hello"))

	line, err = systemdRecordLine(fields, raw, SystemdLineFormatJSON)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(line).EqualTo(raw))

	line, err = systemdRecordLine(fields, raw, SystemdLineFormatShort)
	assert.Requires(a.NilError(err))
	timestamp := time.Unix(1700000000, 0).Local().Format(time.RFC3339Nano)
	assert.Requires(a.String(line).EqualTo(timestamp + " host demo[42]: hello"))
}

func TestSystemdSourceRejectsInvalidLineFormat(t *testing.T) {
	assert := a.New(t)
	_, err := newSystemdSourceWithCommandFactory(SystemdOptions{LineFormat: "bad"}, systemdHelperCommandFactory("", "", 0, false, nil))
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).EqualTo(`systemd line format must be message, short, or json, got "bad"`))
}

func TestSystemdSourceReturnsJournalctlExitErrorsWithStderr(t *testing.T) {
	assert := a.New(t)
	source, err := newSystemdSourceWithCommandFactory(SystemdOptions{}, systemdHelperCommandFactory(
		"",
		"permission denied\n",
		7,
		false,
		nil,
	))
	assert.Requires(a.NilError(err))
	defer func() {
		assert.Requires(a.NilError(source.Close(context.Background())))
	}()

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("journalctl exited: exit status 7"))
	assert.Requires(a.String(err.Error()).Contains(`stderr="permission denied"`))
}

func TestSystemdSourceFollowUsesNoTailAndStopsOnContextCancel(t *testing.T) {
	assert := a.New(t)
	raw := `{"MESSAGE":"live line"}` + "\n"
	var command capturedSystemdCommand
	source, err := newSystemdSourceWithCommandFactory(SystemdOptions{Follow: true}, systemdHelperCommandFactory(
		raw,
		"",
		0,
		true,
		&command,
	))
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
	assert.Requires(a.Slice(command.args).EqualTo("--output=json", "--no-pager", "--all", "--follow", "--no-tail"))

	cancel()
	ok, err = source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
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

type capturedSystemdCommand struct {
	name string
	args []string
}

func systemdHelperCommandFactory(stdout, stderr string, exitCode int, waitForCancel bool, captured *capturedSystemdCommand) systemdCommandFactory {
	return func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if captured != nil {
			captured.name = name
			captured.args = append([]string(nil), args...)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestSystemdSourceHelperProcess")
		cmd.Env = append(os.Environ(),
			"DRAIN_SYSTEMD_HELPER=1",
			"DRAIN_SYSTEMD_STDOUT="+base64.StdEncoding.EncodeToString([]byte(stdout)),
			"DRAIN_SYSTEMD_STDERR="+base64.StdEncoding.EncodeToString([]byte(stderr)),
			"DRAIN_SYSTEMD_EXIT="+strconv.Itoa(exitCode),
			"DRAIN_SYSTEMD_WAIT_CANCEL="+strconv.FormatBool(waitForCancel),
		)
		return cmd
	}
}

func TestSystemdSourceHelperProcess(t *testing.T) {
	if os.Getenv("DRAIN_SYSTEMD_HELPER") != "1" {
		return
	}
	stdout, err := base64.StdEncoding.DecodeString(os.Getenv("DRAIN_SYSTEMD_STDOUT"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	stderr, err := base64.StdEncoding.DecodeString(os.Getenv("DRAIN_SYSTEMD_STDERR"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(stdout)
	_, _ = os.Stderr.Write(stderr)
	if os.Getenv("DRAIN_SYSTEMD_WAIT_CANCEL") == "true" {
		select {}
	}
	code, err := strconv.Atoi(os.Getenv("DRAIN_SYSTEMD_EXIT"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	os.Exit(code)
}
