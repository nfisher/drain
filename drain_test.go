package drain

import (
	"strings"
	"testing"
)

const timestampPrefixPattern = `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`

func TestMaskingRuleMasksTimestampPrefix(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{Pattern: timestampPrefixPattern},
	}
	logger := New(config)

	line := "[Mon May 11 13:41:21 2026] Linux version 6.14.0-1008-nvidia-64k (buildd@bos03-arm64-088) (aarch64-linux-gnu-gcc-13 (Ubuntu 13.3.0-6ubuntu2~24.04) 13.3.0, GNU ld (GNU Binutils for Ubuntu) 2.42) #8-Ubuntu SMP PREEMPT_DYNAMIC Sat Jul 26 02:43:53 UTC 2025 (Ubuntu 6.14.0-1008.8-nvidia-64k 6.14.6)"
	cluster := logger.Train(line)

	template := cluster.getTemplate()
	if strings.Contains(template, "[Mon May 11 13:41:21 2026]") {
		t.Fatalf("template contains fixed timestamp prefix: %q", template)
	}
	if !strings.HasPrefix(template, "<*> Linux version") {
		t.Fatalf("template should start with masked timestamp prefix, got %q", template)
	}

	secondLine := strings.Replace(line, "[Mon May 11 13:41:21 2026]", "[Tue Jun 16 14:42:22 2026]", 1)
	if match := logger.Match(secondLine); match == nil || match.id != cluster.id {
		t.Fatalf("expected changed timestamp prefix to match cluster %d, got %v", cluster.id, match)
	}
}
