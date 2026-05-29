package parseio

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// ObjectStore creates writable objects addressed by slash-separated object paths.
type ObjectStore interface {
	Create(ctx context.Context, objectPath, contentType string) (io.WriteCloser, error)
}

// LocalStore writes objects under a local filesystem prefix.
type LocalStore struct {
	Prefix string
}

func (s LocalStore) Create(_ context.Context, objectPath, _ string) (io.WriteCloser, error) {
	localPath := filepath.Join(s.Prefix, filepath.FromSlash(objectPath))
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return nil, err
	}
	return os.Create(localPath)
}
