package parseio

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"strconv"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

func TestDmesgSourceSnapshotReadsLinesAndReportsInfo(t *testing.T) {
	assert := a.New(t)
	var command capturedDmesgCommand
	source, err := newDmesgSourceWithCommandFactory(false, dmesgHelperCommandFactory(
		"first line\nsecond line\r\nthird",
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
	assert.Requires(a.String(info.Kind).EqualTo("dmesg"))
	assert.Requires(a.String(info.Name).EqualTo("dmesg"))
	assert.Requires(a.True(info.Finite))
	assert.Requires(a.Assert(info.SizeBytes == nil, "dmesg source should not report size bytes"))
	assert.Requires(a.String(command.name).EqualTo(""))

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

	assert.Requires(a.String(command.name).EqualTo("dmesg"))
	assert.Requires(a.Slice(command.args).EqualTo())
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

func TestDmesgSourceReturnsCommandExitErrorsWithStderr(t *testing.T) {
	assert := a.New(t)
	source, err := newDmesgSourceWithCommandFactory(false, dmesgHelperCommandFactory(
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
	assert.Requires(a.String(err.Error()).Contains("dmesg exited: exit status 7"))
	assert.Requires(a.String(err.Error()).Contains(`stderr="permission denied"`))
}

func TestDmesgSourceFollowUsesWatchFlagAndStopsOnContextCancel(t *testing.T) {
	assert := a.New(t)
	var command capturedDmesgCommand
	source, err := newDmesgSourceWithCommandFactory(true, dmesgHelperCommandFactory(
		"live line\n",
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
	assert.Requires(a.String(info.Kind).EqualTo("dmesg"))
	assert.Requires(a.String(info.Name).EqualTo("dmesg"))
	assert.Requires(a.False(info.Finite))

	ctx, cancel := context.WithCancel(context.Background())
	var record SourceRecord
	ok, err := source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.True(ok))
	assert.Requires(a.String(record.Line).EqualTo("live line"))
	assert.Requires(a.String(command.name).EqualTo("dmesg"))
	assert.Requires(a.Slice(command.args).EqualTo("-w"))

	cancel()
	ok, err = source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
}

type capturedDmesgCommand struct {
	name string
	args []string
}

func dmesgHelperCommandFactory(stdout, stderr string, exitCode int, waitForCancel bool, captured *capturedDmesgCommand) dmesgCommandFactory {
	return func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if captured != nil {
			captured.name = name
			captured.args = append([]string(nil), args...)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestDmesgSourceHelperProcess")
		cmd.Env = append(os.Environ(),
			"DRAIN_DMESG_HELPER=1",
			"DRAIN_DMESG_STDOUT="+base64.StdEncoding.EncodeToString([]byte(stdout)),
			"DRAIN_DMESG_STDERR="+base64.StdEncoding.EncodeToString([]byte(stderr)),
			"DRAIN_DMESG_EXIT="+strconv.Itoa(exitCode),
			"DRAIN_DMESG_WAIT_CANCEL="+strconv.FormatBool(waitForCancel),
		)
		return cmd
	}
}

func TestDmesgSourceHelperProcess(t *testing.T) {
	if os.Getenv("DRAIN_DMESG_HELPER") != "1" {
		return
	}
	stdout, err := base64.StdEncoding.DecodeString(os.Getenv("DRAIN_DMESG_STDOUT"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	stderr, err := base64.StdEncoding.DecodeString(os.Getenv("DRAIN_DMESG_STDERR"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(stdout)
	_, _ = os.Stderr.Write(stderr)
	if os.Getenv("DRAIN_DMESG_WAIT_CANCEL") == "true" {
		select {}
	}
	code, err := strconv.Atoi(os.Getenv("DRAIN_DMESG_EXIT"))
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	os.Exit(code)
}
