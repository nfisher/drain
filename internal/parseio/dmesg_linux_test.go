//go:build linux

package parseio

import (
	"context"
	"path/filepath"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

func TestOpenDmesgReaderUsesCustomKmsgPathForSnapshot(t *testing.T) {
	assert := a.New(t)
	path := filepath.Join(t.TempDir(), "missing-kmsg")

	reader, err := openDmesgReader(context.Background(), DmesgOptions{KmsgPath: path})

	assert.Requires(a.Error(err))
	assert.Requires(a.Assert(reader == nil, "reader should be nil when custom kmsg path cannot open"))
	assert.Requires(a.String(err.Error()).Contains("open " + path))
}

func TestOpenDmesgReaderUsesCustomKmsgPathForFollow(t *testing.T) {
	assert := a.New(t)
	path := filepath.Join(t.TempDir(), "missing-kmsg")

	reader, err := openDmesgReader(context.Background(), DmesgOptions{Follow: true, KmsgPath: path})

	assert.Requires(a.Error(err))
	assert.Requires(a.Assert(reader == nil, "reader should be nil when custom kmsg path cannot open"))
	assert.Requires(a.String(err.Error()).Contains("open " + path))
}
