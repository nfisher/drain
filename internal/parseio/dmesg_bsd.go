//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package parseio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sys/unix"
)

func openDmesgReader(_ context.Context, follow bool) (io.ReadCloser, error) {
	if follow {
		return nil, errors.New("direct dmesg follow is not supported on BSD kernels")
	}
	data, err := unix.SysctlRaw("kern.msgbuf")
	if err != nil {
		return nil, fmt.Errorf("read kern.msgbuf: %w", err)
	}
	data = bytes.TrimRight(data, "\x00")
	return io.NopCloser(bytes.NewReader(data)), nil
}
