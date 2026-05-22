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

func TestLoadClustersRestoresAndContinuesTraining(t *testing.T) {
	config := DefaultConfig()
	logger := New(config)
	logger.Train("old cluster line")

	inputSnapshots := []LogClusterSnapshot{
		{
			ID:             7,
			Size:           2,
			TemplateTokens: []string{"user", config.ParamString, "logged", "in"},
		},
	}
	if err := logger.LoadClusters(inputSnapshots); err != nil {
		t.Fatalf("LoadClusters returned error: %v", err)
	}
	inputSnapshots[0].TemplateTokens[1] = "mutated"

	if match := logger.Match("old cluster line"); match != nil {
		t.Fatalf("LoadClusters should replace existing clusters, got %v", match)
	}

	match := logger.Match("user alice logged in")
	if match == nil {
		t.Fatal("expected restored cluster to match")
	}
	if match.id != 7 {
		t.Fatalf("expected restored cluster id 7, got %d", match.id)
	}
	if match.size != 2 {
		t.Fatalf("expected restored cluster size 2, got %d", match.size)
	}

	snapshots := logger.ClusterSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("expected one snapshot, got %d", len(snapshots))
	}
	if snapshots[0].ID != 7 || snapshots[0].Size != 2 {
		t.Fatalf("snapshot did not preserve id/size: %+v", snapshots[0])
	}
	if snapshots[0].TemplateTokens[1] != config.ParamString {
		t.Fatalf("LoadClusters did not copy template tokens: %+v", snapshots[0].TemplateTokens)
	}
	snapshots[0].TemplateTokens[1] = "mutated again"
	if match := logger.Match("user bob logged in"); match == nil || match.id != 7 {
		t.Fatalf("ClusterSnapshots should return token copies, got %v", match)
	}

	updated := logger.Train("user carol logged in")
	if updated.id != 7 {
		t.Fatalf("expected existing cluster id 7 to update, got %d", updated.id)
	}
	if updated.size != 3 {
		t.Fatalf("expected existing cluster size 3, got %d", updated.size)
	}

	created := logger.Train("connected to 10.0.0.1")
	if created.id != 8 {
		t.Fatalf("expected new cluster id after restored max to be 8, got %d", created.id)
	}
}

func TestLoadClustersValidatesSnapshots(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
		snapshots []LogClusterSnapshot
	}{
		{
			name: "duplicate id",
			snapshots: []LogClusterSnapshot{
				{ID: 1, Size: 1, TemplateTokens: []string{"a"}},
				{ID: 1, Size: 1, TemplateTokens: []string{"b"}},
			},
		},
		{
			name: "non-positive id",
			snapshots: []LogClusterSnapshot{
				{ID: 0, Size: 1, TemplateTokens: []string{"a"}},
			},
		},
		{
			name: "non-positive size",
			snapshots: []LogClusterSnapshot{
				{ID: 1, Size: 0, TemplateTokens: []string{"a"}},
			},
		},
		{
			name: "max clusters too small",
			configure: func(config *Config) {
				config.MaxClusters = 1
			},
			snapshots: []LogClusterSnapshot{
				{ID: 1, Size: 1, TemplateTokens: []string{"a"}},
				{ID: 2, Size: 1, TemplateTokens: []string{"b"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			if tt.configure != nil {
				tt.configure(config)
			}
			logger := New(config)
			logger.Train("kept line")

			if err := logger.LoadClusters(tt.snapshots); err == nil {
				t.Fatal("expected LoadClusters error")
			}
			if match := logger.Match("kept line"); match == nil {
				t.Fatal("LoadClusters error should not replace existing clusters")
			}
		})
	}
}
