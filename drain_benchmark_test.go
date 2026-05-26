package drain

import (
	"fmt"
	"testing"
)

var benchmarkDrainCluster *LogCluster

func benchmarkDrainConfig() *Config {
	config := DefaultConfig()
	config.LogClusterDepth = 6
	config.MaskingRules = []MaskingRule{
		{Pattern: timestampPrefixPattern},
	}
	return config
}

func benchmarkDrainLines(count int) []string {
	lines := make([]string, count)
	for i := range lines {
		timestamp := fmt.Sprintf("[Mon May %02d 13:%02d:%02d 2026]", i%28+1, i%60, (i*7)%60)
		switch i % 6 {
		case 0:
			lines[i] = fmt.Sprintf("%s service api host web-%03d request r-%06d method GET path /v1/users/%d status 200 latency_ms %d", timestamp, i%64, i, 1000+i, 5+i%90)
		case 1:
			lines[i] = fmt.Sprintf("%s service api host web-%03d request r-%06d method POST path /v1/orders/%d status 201 latency_ms %d", timestamp, i%64, i, 2000+i, 12+i%120)
		case 2:
			lines[i] = fmt.Sprintf("%s service worker host worker-%03d job j-%06d queue email status done duration_ms %d", timestamp, i%32, i, 20+i%80)
		case 3:
			lines[i] = fmt.Sprintf("%s service worker host worker-%03d job j-%06d queue billing status retry duration_ms %d", timestamp, i%32, i, 30+i%100)
		case 4:
			lines[i] = fmt.Sprintf("%s service db host db-%02d query q-%06d table users operation select rows %d duration_ms %d", timestamp, i%12, i, 10+i%500, 3+i%50)
		default:
			lines[i] = fmt.Sprintf("%s service auth host auth-%02d user user-%06d action login result success duration_ms %d", timestamp, i%8, i, 1+i%30)
		}
	}
	return lines
}

func BenchmarkDrainTrain(b *testing.B) {
	lines := benchmarkDrainLines(2048)
	logger := New(benchmarkDrainConfig())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkDrainCluster = logger.Train(lines[i%len(lines)])
	}
}

func BenchmarkDrainMatchRestoredClusters(b *testing.B) {
	lines := benchmarkDrainLines(2048)
	trained := New(benchmarkDrainConfig())
	for _, line := range lines {
		trained.Train(line)
	}

	logger := New(benchmarkDrainConfig())
	if err := logger.LoadClusters(trained.ClusterSnapshots()); err != nil {
		b.Fatalf("load clusters: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkDrainCluster = logger.Match(lines[i%len(lines)])
		if benchmarkDrainCluster == nil {
			b.Fatalf("line did not match: %q", lines[i%len(lines)])
		}
	}
}

func BenchmarkDrainLoadClusters(b *testing.B) {
	lines := benchmarkDrainLines(2048)
	trained := New(benchmarkDrainConfig())
	for _, line := range lines {
		trained.Train(line)
	}
	snapshots := trained.ClusterSnapshots()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger := New(benchmarkDrainConfig())
		if err := logger.LoadClusters(snapshots); err != nil {
			b.Fatalf("load clusters: %v", err)
		}
	}
}
