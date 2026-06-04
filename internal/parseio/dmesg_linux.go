//go:build linux

package parseio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	linuxKmsgReadBufferSize = 256 * 1024
	linuxKmsgPollMillis     = 250
	linuxKmsgMicrosPerSec   = 1000 * 1000
)

var errLinuxKmsgNoData = errors.New("no kernel messages available")

func openDmesgReader(ctx context.Context, options DmesgOptions) (io.ReadCloser, error) {
	options = normalizeDmesgOptions(options)
	if options.Follow {
		stream, err := newLinuxKmsgStream(ctx, options.KmsgPath)
		if err != nil {
			return nil, err
		}
		return stream, nil
	}
	return newLinuxKmsgSnapshotReader(options.KmsgPath)
}

func newLinuxKmsgSnapshotReader(path string) (io.ReadCloser, error) {
	data, err := readLinuxKmsgSnapshot(path)
	if err != nil {
		if path == DefaultDmesgKmsgPath && errors.Is(err, os.ErrNotExist) {
			return newLinuxKlogSnapshotReader()
		}
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func readLinuxKmsgSnapshot(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = unix.Close(fd)
	}()

	if _, err := unix.Seek(fd, 0, unix.SEEK_SET); err != nil {
		return nil, fmt.Errorf("seek %s: %w", path, err)
	}

	var out bytes.Buffer
	buf := make([]byte, linuxKmsgReadBufferSize)
	for {
		raw, err := readLinuxKmsgRecord(path, fd, buf)
		if err != nil {
			if errors.Is(err, errLinuxKmsgNoData) {
				return out.Bytes(), nil
			}
			return nil, err
		}
		out.WriteString(formatLinuxKmsgRecord(raw))
		out.WriteByte('\n')
	}
}

func newLinuxKlogSnapshotReader() (io.ReadCloser, error) {
	size, err := unix.Klogctl(unix.SYSLOG_ACTION_SIZE_BUFFER, nil)
	if err != nil {
		return nil, fmt.Errorf("read kernel log buffer size: %w", err)
	}
	if size <= 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}

	buf := make([]byte, size)
	n, err := unix.Klogctl(unix.SYSLOG_ACTION_READ_ALL, buf)
	if err != nil {
		return nil, fmt.Errorf("read kernel log buffer: %w", err)
	}
	return io.NopCloser(bytes.NewReader(buf[:n])), nil
}

type linuxKmsgStream struct {
	ctx  context.Context
	path string
	fd   int

	readBuffer []byte
	pending    bytes.Buffer
	closed     bool
}

func newLinuxKmsgStream(ctx context.Context, path string) (*linuxKmsgStream, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if _, err := unix.Seek(fd, 0, unix.SEEK_END); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("seek %s: %w", path, err)
	}
	return &linuxKmsgStream{
		ctx:        ctx,
		path:       path,
		fd:         fd,
		readBuffer: make([]byte, linuxKmsgReadBufferSize),
	}, nil
}

func (r *linuxKmsgStream) Read(p []byte) (int, error) {
	if r.pending.Len() > 0 {
		return r.pending.Read(p)
	}
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		raw, err := readLinuxKmsgRecord(r.path, r.fd, r.readBuffer)
		if err == nil {
			r.pending.WriteString(formatLinuxKmsgRecord(raw))
			r.pending.WriteByte('\n')
			return r.pending.Read(p)
		}
		if !errors.Is(err, errLinuxKmsgNoData) {
			return 0, err
		}
		if err := waitLinuxKmsgRecord(r.ctx, r.path, r.fd); err != nil {
			return 0, err
		}
	}
}

func (r *linuxKmsgStream) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return unix.Close(r.fd)
}

func readLinuxKmsgRecord(path string, fd int, buf []byte) (string, error) {
	for {
		n, err := unix.Read(fd, buf)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EPIPE) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				return "", errLinuxKmsgNoData
			}
			return "", fmt.Errorf("read %s: %w", path, err)
		}
		if n == 0 {
			return "", io.EOF
		}
		return string(buf[:n]), nil
	}
}

func waitLinuxKmsgRecord(ctx context.Context, path string, fd int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fds := []unix.PollFd{{
			Fd:     int32(fd),
			Events: unix.POLLIN,
		}}
		n, err := unix.Poll(fds, linuxKmsgPollMillis)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("poll %s: %w", path, err)
		}
		if n == 0 {
			continue
		}
		if fds[0].Revents&unix.POLLNVAL != 0 {
			return os.ErrClosed
		}
		if fds[0].Revents&(unix.POLLIN|unix.POLLERR|unix.POLLHUP) != 0 {
			return nil
		}
	}
}

func formatLinuxKmsgRecord(raw string) string {
	raw = strings.TrimRight(raw, "\x00\r\n")
	metadata, message, ok := strings.Cut(raw, ";")
	if !ok {
		return raw
	}

	fields := strings.Split(metadata, ",")
	if len(fields) < 3 {
		return message
	}
	timestampMicros, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || timestampMicros < 0 {
		return message
	}
	seconds := timestampMicros / linuxKmsgMicrosPerSec
	micros := timestampMicros % linuxKmsgMicrosPerSec
	return fmt.Sprintf("[%5d.%06d] %s", seconds, micros, message)
}
