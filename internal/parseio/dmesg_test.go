package parseio

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

func TestDmesgSourceSnapshotReadsLinesAndReportsInfo(t *testing.T) {
	assert := a.New(t)
	var reader capturedDmesgReader
	source, err := newDmesgSourceWithReaderFactory(false, dmesgStringReaderFactory(
		"first line\nsecond line\r\nthird",
		&reader,
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
	assert.Requires(a.Number(reader.starts).EqualTo(0))

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

	assert.Requires(a.Number(reader.starts).EqualTo(1))
	assert.Requires(a.False(reader.follow))
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

func TestDmesgSourceReturnsReaderOpenErrors(t *testing.T) {
	assert := a.New(t)
	openErr := errors.New("permission denied")
	source, err := newDmesgSourceWithReaderFactory(false, func(context.Context, bool) (io.ReadCloser, error) {
		return nil, openErr
	})
	assert.Requires(a.NilError(err))
	defer func() {
		assert.Requires(a.NilError(source.Close(context.Background())))
	}()

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.True(errors.Is(err, openErr)))
}

func TestDmesgSourceFollowStopsOnContextCancel(t *testing.T) {
	assert := a.New(t)
	var reader capturedDmesgReader
	source, err := newDmesgSourceWithReaderFactory(true, func(ctx context.Context, follow bool) (io.ReadCloser, error) {
		reader.starts++
		reader.follow = follow
		return &cancelingDmesgReader{
			ctx:   ctx,
			input: strings.NewReader("live line\n"),
		}, nil
	})
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
	assert.Requires(a.Number(reader.starts).EqualTo(1))
	assert.Requires(a.True(reader.follow))

	cancel()
	ok, err = source.Next(ctx, &record)
	assert.Requires(a.NilError(err))
	assert.Requires(a.False(ok))
}

type capturedDmesgReader struct {
	starts int
	follow bool
}

func dmesgStringReaderFactory(input string, captured *capturedDmesgReader) dmesgReaderFactory {
	return func(_ context.Context, follow bool) (io.ReadCloser, error) {
		if captured != nil {
			captured.starts++
			captured.follow = follow
		}
		return io.NopCloser(strings.NewReader(input)), nil
	}
}

type cancelingDmesgReader struct {
	ctx   context.Context
	input *strings.Reader
}

func (r *cancelingDmesgReader) Read(p []byte) (int, error) {
	if r.input.Len() > 0 {
		return r.input.Read(p)
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (r *cancelingDmesgReader) Close() error {
	return nil
}
