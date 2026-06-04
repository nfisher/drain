//go:build !linux || !cgo

package parseio

import "errors"

func openSystemdJournal() (systemdJournal, error) {
	return nil, errors.New("systemd journal source requires Linux with cgo and libsystemd")
}
