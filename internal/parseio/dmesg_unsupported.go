//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package parseio

import (
	"context"
	"errors"
	"io"
	"runtime"
)

func openDmesgReader(context.Context, DmesgOptions) (io.ReadCloser, error) {
	return nil, errors.New("direct dmesg source is not supported on " + runtime.GOOS)
}

func formatDmesgRecord(raw string) string {
	return raw
}
