package parseio

import (
	"strings"
	"testing"
	"time"
)

func TestValidateSinkOptionsAcceptsSupportedFormats(t *testing.T) {
	for _, format := range []string{parseFormatJSONL, parseFormatParquet} {
		if err := ValidateSinkOptions(format, 1, time.Nanosecond); err != nil {
			t.Fatalf("ValidateSinkOptions(%q) error = %v", format, err)
		}
	}
}

func TestValidateSinkOptionsRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		batchSize   int
		batchMaxAge time.Duration
		want        string
	}{
		{name: "format", format: "xml", batchSize: 1, batchMaxAge: time.Second, want: "format must be"},
		{name: "batch size", format: parseFormatJSONL, batchSize: 0, batchMaxAge: time.Second, want: "batch-size must be greater than 0"},
		{name: "batch max age", format: parseFormatJSONL, batchSize: 1, batchMaxAge: 0, want: "batch-max-age must be greater than 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSinkOptions(tt.format, tt.batchSize, tt.batchMaxAge)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateSinkOptions() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestValidateSystemdOptionsRejectsInvalidLineFormatWithoutOpeningJournal(t *testing.T) {
	err := ValidateSystemdOptions(SystemdOptions{LineFormat: "xml"})
	if err == nil || !strings.Contains(err.Error(), "systemd line format") {
		t.Fatalf("ValidateSystemdOptions() error = %v, want invalid line-format", err)
	}
}

func TestValidateSystemdOptionsRejectsInvalidPriority(t *testing.T) {
	err := ValidateSystemdOptions(SystemdOptions{Priority: "loud"})
	if err == nil || !strings.Contains(err.Error(), "systemd priority") {
		t.Fatalf("ValidateSystemdOptions() error = %v, want invalid priority", err)
	}
}
