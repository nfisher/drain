//go:build !linux || !cgo

package parseio

import (
	"context"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

func TestSystemdSourceUnsupportedBackendReturnsClearError(t *testing.T) {
	assert := a.New(t)
	source, err := NewSystemdSource(SystemdOptions{})
	assert.Requires(a.NilError(err))

	var record SourceRecord
	ok, err := source.Next(context.Background(), &record)
	assert.Requires(a.False(ok))
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).EqualTo("systemd journal source requires Linux with cgo and libsystemd"))
}
