package parseio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// SourceCheckpointer is implemented by sources that can resume from and report
// durable read positions.
type SourceCheckpointer interface {
	Resume(SourceCheckpoint) error
	Checkpoint() SourceCheckpoint
}

// SourceCheckpoint stores the last successfully acknowledged source position.
type SourceCheckpoint struct {
	Kind      string             `json:"kind,omitempty"`
	Name      string             `json:"name,omitempty"`
	Locator   map[string]string  `json:"locator,omitempty"`
	UpdatedAt time.Time          `json:"updated_at,omitempty"`
	File      *FileCheckpoint    `json:"file,omitempty"`
	Dmesg     *DmesgCheckpoint   `json:"dmesg,omitempty"`
	Systemd   *SystemdCheckpoint `json:"systemd,omitempty"`
}

// FileCheckpoint records the next file position to read.
type FileCheckpoint struct {
	ByteOffset int64 `json:"byte_offset"`
	LineNumber int64 `json:"line_number"`
}

// DmesgCheckpoint records the last acknowledged kernel message cursor.
type DmesgCheckpoint struct {
	Cursor     string `json:"cursor,omitempty"`
	ByteOffset int64  `json:"byte_offset"`
	LineNumber int64  `json:"line_number"`
}

// SystemdCheckpoint records the last acknowledged journal cursor.
type SystemdCheckpoint struct {
	Cursor string `json:"cursor,omitempty"`
}

// LoadSourceCheckpoint reads a checkpoint file. Missing files return an empty
// checkpoint so first starts do not need special handling.
func LoadSourceCheckpoint(path string) (SourceCheckpoint, error) {
	file, err := os.Open(path) // #nosec G304 -- checkpoint path is an explicit CLI/config input.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return SourceCheckpoint{}, nil
		}
		return SourceCheckpoint{}, err
	}
	defer func() { _ = file.Close() }()

	var checkpoint SourceCheckpoint
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return SourceCheckpoint{}, fmt.Errorf("decode checkpoint %s: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return SourceCheckpoint{}, fmt.Errorf("decode checkpoint %s: trailing data", path)
	}
	return checkpoint, nil
}

// SaveSourceCheckpoint atomically writes a checkpoint file.
func SaveSourceCheckpoint(ctx context.Context, path string, checkpoint SourceCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	checkpoint.UpdatedAt = time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	encoder := json.NewEncoder(tmp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(checkpoint); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
