//go:build linux && cgo

package parseio

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
)

const systemdJournalWaitInterval = 250 * time.Millisecond

type sdJournalBackend struct {
	journal *sdjournal.Journal
}

func openSystemdJournal() (systemdJournal, error) {
	journal, err := sdjournal.NewJournal()
	if err != nil {
		return nil, fmt.Errorf("open systemd journal: %w", err)
	}
	if err := journal.SetDataThreshold(0); err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("configure systemd journal data threshold: %w", err)
	}
	return &sdJournalBackend{journal: journal}, nil
}

func (j *sdJournalBackend) Close() error {
	return j.journal.Close()
}

func (j *sdJournalBackend) AddMatch(match string) error {
	return j.journal.AddMatch(match)
}

func (j *sdJournalBackend) GetBootID() (string, error) {
	return j.journal.GetBootID()
}

func (j *sdJournalBackend) SeekHead() error {
	return j.journal.SeekHead()
}

func (j *sdJournalBackend) SeekRealtimeUsec(usec uint64) error {
	return j.journal.SeekRealtimeUsec(usec)
}

func (j *sdJournalBackend) SeekCursor(cursor string) error {
	return j.journal.SeekCursor(cursor)
}

func (j *sdJournalBackend) Next() (bool, error) {
	advanced, err := j.journal.Next()
	return advanced > 0, err
}

func (j *sdJournalBackend) Entry() (systemdJournalEntry, error) {
	entry, err := j.journal.GetEntry()
	if err != nil {
		return systemdJournalEntry{}, err
	}
	return systemdJournalEntry{
		Fields:             entry.Fields,
		Cursor:             entry.Cursor,
		RealtimeTimestamp:  entry.RealtimeTimestamp,
		MonotonicTimestamp: entry.MonotonicTimestamp,
	}, nil
}

func (j *sdJournalBackend) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		state := j.journal.Wait(systemdJournalWaitInterval)
		switch state {
		case sdjournal.SD_JOURNAL_APPEND, sdjournal.SD_JOURNAL_INVALIDATE:
			return nil
		case sdjournal.SD_JOURNAL_NOP:
			continue
		default:
			if state < 0 {
				return fmt.Errorf("wait for systemd journal: %d", state)
			}
			return nil
		}
	}
}
