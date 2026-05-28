package drain

import (
	"reflect"
	"strconv"
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

func TestMaskingRuleUsesLiteralReplacement(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `user-\d+`,
			MaskWith: `$user`,
		},
	}
	logger := New(config)

	cluster := logger.Train("service user-123 ready")

	if got := cluster.getTemplate(); got != "service $user ready" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "service $user ready", got)
	}
	if match := logger.Match("service user-456 ready"); match == nil || match.id != cluster.id {
		t.Fatalf("expected changed user id to match cluster %d, got %v", cluster.id, match)
	}
}

func TestMaskingRuleUsesNamedReplacement(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `\b\d{1,3}(?:\.\d{1,3}){3}\b`,
			MaskWith: `IP`,
		},
	}
	logger := New(config)

	cluster := logger.Train("connected to 10.0.0.1")

	if got := cluster.Template(); got != "connected to <:IP:>" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "connected to <:IP:>", got)
	}
	if match := logger.Match("connected to 192.168.0.1"); match == nil || match.id != cluster.id {
		t.Fatalf("expected changed IP to match cluster %d, got %v", cluster.id, match)
	}
}

func TestMaskingRuleKeepsExplicitLiteralReplacement(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `user-\d+`,
			MaskWith: `<user>`,
		},
	}
	logger := New(config)

	cluster := logger.Train("service user-123 ready")

	if got := cluster.Template(); got != "service <user> ready" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "service <user> ready", got)
	}
}

func TestExtractParametersReturnsNamedAndEmbeddedRawValues(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `\d+`,
			MaskWith: `NUM`,
		},
	}
	logger := New(config)

	cluster := logger.Train("service id=123 path=/users/42 status ok")
	cluster = logger.Train("service id=456 path=/users/84 status failed")

	if got, want := cluster.Template(), "service id=<:NUM:> path=/users/<:NUM:> status <*>"; got != want {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", want, got)
	}
	parameters, ok := logger.ExtractParameters(cluster.Template(), "service id=789 path=/users/99 status retry")
	if !ok {
		t.Fatal("expected template extraction to match")
	}
	want := []ExtractedParameter{
		{Value: "789", MaskName: "NUM"},
		{Value: "99", MaskName: "NUM"},
		{Value: "retry", MaskName: "*"},
	}
	if !reflect.DeepEqual(parameters, want) {
		t.Fatalf("parameters mismatch:\nwant %#v\ngot  %#v", want, parameters)
	}
}

func TestExtractParametersHandlesWhitespaceAndMaskedValuesWithSpaces(t *testing.T) {
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{Pattern: timestampPrefixPattern},
	}
	logger := New(config)

	cluster := logger.Train("[Mon May 11 13:41:21 2026] user alice logged in")
	cluster = logger.Train("[Tue Jun 16 14:42:22 2026] user bob logged in")

	parameters, ok := logger.ExtractParameters(cluster.Template(), "[Wed Jul 17 15:43:23 2026]\t user   carol logged\tin")
	if !ok {
		t.Fatal("expected template extraction to match")
	}
	want := []ExtractedParameter{
		{Value: "[Wed Jul 17 15:43:23 2026]", MaskName: "*"},
		{Value: "carol", MaskName: "*"},
	}
	if !reflect.DeepEqual(parameters, want) {
		t.Fatalf("parameters mismatch:\nwant %#v\ngot  %#v", want, parameters)
	}
}

func TestSingleMaskTokenizationMatchesSequentialNoopRule(t *testing.T) {
	rule := MaskingRule{
		Pattern:  `\d+`,
		MaskWith: `$value with space`,
	}
	fastRules := []MaskingRule{rule}
	fallbackRules := []MaskingRule{
		rule,
		{Pattern: `__drain_never_matches__`},
	}

	for _, line := range []string{
		"123 service ready",
		"service id 123 ready",
		"service id=123 ready",
		"service ready",
		"   ",
		"alpha  123  beta",
		"123",
		"a123b",
	} {
		t.Run(line, func(t *testing.T) {
			fast := tokensForMaskingRules(t, line, fastRules)
			fallback := tokensForMaskingRules(t, line, fallbackRules)
			if !reflect.DeepEqual(fast, fallback) {
				t.Fatalf("tokens mismatch:\nfast     %#v\nfallback %#v", fast, fallback)
			}
		})
	}
}

func TestContentTokenizationUsesDrain3Whitespace(t *testing.T) {
	logger := New(DefaultConfig())

	got := logger.getContentAsTokens(" \talpha  beta\tgamma\n")
	want := []string{"alpha", "beta", "gamma"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
	}

	if got := logger.getContentAsTokens(" \t "); len(got) != 0 {
		t.Fatalf("blank input should produce zero tokens, got %#v", got)
	}
}

func TestContentTokenizationUsesExtraDelimiters(t *testing.T) {
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_", ":"}
	logger := New(config)

	got := logger.getContentAsTokens(" \talpha__beta:gamma  delta::epsilon\n")
	want := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
	}
}

func TestExtraDelimitersApplyAfterMasking(t *testing.T) {
	config := DefaultConfig()
	config.ExtraDelimiters = []string{":"}
	config.MaskingRules = []MaskingRule{{Pattern: timestampPrefixPattern}}
	logger := New(config)

	got := logger.getContentAsTokens("[Mon May 11 13:41:21 2026]:user:alice")
	want := []string{"<*>", "user", "alice"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
	}
}

func TestNewRejectsEmptyExtraDelimiter(t *testing.T) {
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_", ""}

	defer func() {
		recovered := recover()
		message, _ := recovered.(string)
		if recovered == nil || !strings.Contains(message, "extra delimiter must not be empty") {
			t.Fatalf("expected empty extra delimiter panic, got %v", recovered)
		}
	}()
	New(config)
}

func TestTrainSimilarityIgnoresWhitespaceRuns(t *testing.T) {
	logger := New(DefaultConfig())
	cluster := logger.Train("user  alice\tlogged in")

	updated := logger.Train("user bob logged in")

	if updated != cluster {
		t.Fatalf("expected whitespace-normalized line to update cluster %d, got %v", cluster.id, updated)
	}
	if got := updated.getTemplate(); got != "user <*> logged in" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "user <*> logged in", got)
	}
}

func TestTrainAndMatchUseExtraDelimiters(t *testing.T) {
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_"}
	logger := New(config)

	cluster := logger.Train("user_alice_logged_in")
	updated := logger.Train("user_bob_logged_in")

	if updated != cluster {
		t.Fatalf("expected delimiter-normalized line to update cluster %d, got %v", cluster.id, updated)
	}
	if got := updated.getTemplate(); got != "user <*> logged in" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "user <*> logged in", got)
	}
	if match := logger.Match("user_carol_logged_in"); match == nil || match.id != cluster.id {
		t.Fatalf("expected delimiter-normalized line to match cluster %d, got %v", cluster.id, match)
	}
}

func TestExtractParametersPreservesMaskedDelimiterValues(t *testing.T) {
	config := DefaultConfig()
	config.ExtraDelimiters = []string{":"}
	config.MaskingRules = []MaskingRule{{Pattern: timestampPrefixPattern}}
	logger := New(config)
	cluster := logger.Train("[Mon May 11 13:41:21 2026]:user:alice:logged:in")
	cluster = logger.Train("[Tue Jun 16 14:42:22 2026]:user:bob:logged:in")

	parameters, ok := logger.ExtractParameters(cluster.Template(), "[Wed Jul 17 15:43:23 2026]:user:carol:logged:in")
	if !ok {
		t.Fatal("expected template extraction to match")
	}
	want := []ExtractedParameter{
		{Value: "[Wed Jul 17 15:43:23 2026]", MaskName: "*"},
		{Value: "carol", MaskName: "*"},
	}
	if !reflect.DeepEqual(parameters, want) {
		t.Fatalf("parameters mismatch:\nwant %#v\ngot  %#v", want, parameters)
	}
}

func TestBlankInputProducesZeroTokenCluster(t *testing.T) {
	logger := New(DefaultConfig())

	cluster := logger.Train(" \t ")

	if len(cluster.logTemplateTokens) != 0 {
		t.Fatalf("blank input should produce zero template tokens, got %#v", cluster.logTemplateTokens)
	}
	if match := logger.Match("\t  "); match == nil || match.id != cluster.id {
		t.Fatalf("expected blank input to match cluster %d, got %v", cluster.id, match)
	}
}

func TestMatchKeepsTreeOnlySearchByDefault(t *testing.T) {
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "fixed", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "target", "ready"}},
	})

	if match := logger.Match("alpha target ready"); match != nil {
		t.Fatalf("tree-only Match should miss wildcard branch, got %v", match)
	}
}

func TestMatchWithOptionsFallbackFindsWildcardBranch(t *testing.T) {
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "fixed", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "target", "ready"}},
	})

	match := logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchFallback,
	})

	if match == nil || match.id != 2 {
		t.Fatalf("fallback should find wildcard cluster 2, got %v", match)
	}
}

func TestMatchWithOptionsAlwaysSearchesAllSameLengthClusters(t *testing.T) {
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "target", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "<*>", "ready"}},
	})

	if match := logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchFallback,
	}); match == nil || match.id != 1 {
		t.Fatalf("fallback should keep tree match cluster 1, got %v", match)
	}

	match := logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchAlways,
	})
	if match == nil || match.id != 2 {
		t.Fatalf("always should scan every same-length cluster and select cluster 2, got %v", match)
	}
}

func TestMatchWithOptionsRejectsInvalidFullSearchStrategy(t *testing.T) {
	logger := New(DefaultConfig())
	logger.Train("alpha target ready")

	defer func() {
		if recover() == nil {
			t.Fatal("expected invalid full search strategy to panic")
		}
	}()
	logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchStrategy("sometimes"),
	})
}

func TestMatchWithOptionsFullSearchHandlesBlankInput(t *testing.T) {
	logger := New(DefaultConfig())
	cluster := logger.Train(" \t ")

	match := logger.MatchWithOptions(" ", MatchOptions{
		FullSearchStrategy: FullSearchAlways,
	})

	if match == nil || match.id != cluster.id {
		t.Fatalf("always should match blank cluster %d, got %v", cluster.id, match)
	}
}

func TestMatchDoesNotRefreshClusterCacheRecency(t *testing.T) {
	config := DefaultConfig()
	config.MaxClusters = 2
	logger := New(config)

	first := logger.Train("alpha one")
	logger.Train("beta two")
	if match := logger.Match("alpha one"); match == nil || match.id != first.id {
		t.Fatalf("expected alpha line to match cluster %d, got %v", first.id, match)
	}

	logger.Train("gamma three")

	if match := logger.Match("alpha one"); match != nil {
		t.Fatalf("Match should not keep alpha cluster hot, got %v", match)
	}
	if match := logger.Match("beta two"); match == nil {
		t.Fatal("expected beta cluster to remain cached")
	}
}

func TestTrainRefreshesAcceptedClusterCacheRecency(t *testing.T) {
	config := DefaultConfig()
	config.MaxClusters = 2
	logger := New(config)

	first := logger.Train("alpha one")
	second := logger.Train("beta two")
	if updated := logger.Train("alpha one"); updated != first {
		t.Fatalf("expected alpha cluster %d to be updated, got %v", first.id, updated)
	}

	logger.Train("gamma three")

	if match := logger.Match("alpha one"); match == nil || match.id != first.id {
		t.Fatalf("Train should keep alpha cluster hot, got %v", match)
	}
	if match := logger.Match("beta two"); match != nil {
		t.Fatalf("expected beta cluster %d to be evicted, got %v", second.id, match)
	}
}

func TestTrainKeepsTemplateTokensWhenTemplateIsUnchanged(t *testing.T) {
	logger := New(DefaultConfig())
	cluster := logger.Train("fixed line")
	before := cluster.logTemplateTokens

	updated := logger.Train("fixed line")

	if updated != cluster {
		t.Fatalf("expected same cluster to be updated")
	}
	if updated.size != 2 {
		t.Fatalf("expected cluster size 2, got %d", updated.size)
	}
	if !sameTokenBacking(before, updated.logTemplateTokens) {
		t.Fatalf("expected unchanged template to reuse token backing array")
	}
	if got := updated.getTemplate(); got != "fixed line" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "fixed line", got)
	}
}

func TestTrainKeepsTemplateTokensWhenAlreadyGeneralized(t *testing.T) {
	logger := New(DefaultConfig())
	cluster := logger.Train("user alice logged in")
	cluster = logger.Train("user bob logged in")
	if got := cluster.getTemplate(); got != "user <*> logged in" {
		t.Fatalf("template mismatch after generalization:\nwant %q\ngot  %q", "user <*> logged in", got)
	}
	before := cluster.logTemplateTokens

	updated := logger.Train("user carol logged in")

	if updated != cluster {
		t.Fatalf("expected same cluster to be updated")
	}
	if updated.size != 3 {
		t.Fatalf("expected cluster size 3, got %d", updated.size)
	}
	if !sameTokenBacking(before, updated.logTemplateTokens) {
		t.Fatalf("expected already-generalized template to reuse token backing array")
	}
	if got := updated.getTemplate(); got != "user <*> logged in" {
		t.Fatalf("template mismatch after unchanged update:\nwant %q\ngot  %q", "user <*> logged in", got)
	}
	if match := logger.Match("user dave logged in"); match == nil || match.id != cluster.id {
		t.Fatalf("expected generalized template to match cluster %d, got %v", cluster.id, match)
	}
}

func TestNumericTokensAreParameterizedByDefault(t *testing.T) {
	config := DefaultConfig()
	config.LogClusterDepth = 5
	logger := New(config)

	first := logger.Train("host web-001 ready")
	second := logger.Train("host web-002 ready")

	if second != first {
		t.Fatalf("expected numeric token variants to share cluster %d, got %v", first.id, second)
	}
	if got := second.getTemplate(); got != "host <*> ready" {
		t.Fatalf("template mismatch:\nwant %q\ngot  %q", "host <*> ready", got)
	}
}

func TestPreserveNumericTokensKeepsNumericPrefixesExact(t *testing.T) {
	config := DefaultConfig()
	config.LogClusterDepth = 5
	config.PreserveNumericTokens = true
	logger := New(config)

	first := logger.Train("host web-001 ready")
	second := logger.Train("host web-002 ready")

	if second == first {
		t.Fatalf("expected preserved numeric token to create a distinct cluster, got %v", second)
	}
	if got := len(logger.Clusters()); got != 2 {
		t.Fatalf("expected two clusters, got %d", got)
	}
}

func TestMaxChildrenSpillsDistinctBranchesToWildcardAtLimit(t *testing.T) {
	config := DefaultConfig()
	config.LogClusterDepth = 5
	config.MaxChildren = 3
	logger := New(config)

	logger.Train("alpha common tail")
	logger.Train("beta common tail")
	spillCluster := logger.Train("gamma common tail")
	updatedSpillCluster := logger.Train("delta common tail")

	if updatedSpillCluster != spillCluster {
		t.Fatalf("expected later distinct token to reuse wildcard branch cluster %d, got %v", spillCluster.id, updatedSpillCluster)
	}
	if got := updatedSpillCluster.Template(); got != "<*> common tail" {
		t.Fatalf("wildcard branch template mismatch:\nwant %q\ngot  %q", "<*> common tail", got)
	}

	tokenCountNode := childNode(t, logger.rootNode, "3")
	assertChildKeys(t, tokenCountNode, []string{"<*>", "alpha", "beta"})
	if _, ok := tokenCountNode.keyToChildNode["gamma"]; ok {
		t.Fatal("max children limit should route gamma through wildcard instead of adding a literal child")
	}
	if _, ok := tokenCountNode.keyToChildNode["delta"]; ok {
		t.Fatal("max children limit should route delta through wildcard instead of adding a literal child")
	}
}

func TestMaxChildrenCountsExistingWildcardChild(t *testing.T) {
	config := DefaultConfig()
	config.LogClusterDepth = 5
	config.MaxChildren = 3
	logger := New(config)

	logger.Train("node1 a b c")
	logger.Train("alpha d e f")
	logger.Train("beta g h i")
	logger.Train("gamma j k l")

	tokenCountNode := childNode(t, logger.rootNode, "4")
	assertChildKeys(t, tokenCountNode, []string{"<*>", "alpha", "beta"})
	if _, ok := tokenCountNode.keyToChildNode["gamma"]; ok {
		t.Fatal("existing wildcard child should count toward MaxChildren")
	}
}

func TestNewRejectsNonPositiveMaxChildren(t *testing.T) {
	for _, maxChildren := range []int{0, -1} {
		t.Run(strconv.Itoa(maxChildren), func(t *testing.T) {
			config := DefaultConfig()
			config.MaxChildren = maxChildren

			defer func() {
				recovered := recover()
				message, _ := recovered.(string)
				if recovered == nil || !strings.Contains(message, "max children must be at least 1") {
					t.Fatalf("expected max children panic, got %v", recovered)
				}
			}()
			New(config)
		})
	}
}

func TestLogClusterDepthBoundsPrefixTree(t *testing.T) {
	shallowConfig := DefaultConfig()
	shallowConfig.LogClusterDepth = 4
	shallow := New(shallowConfig)
	shallow.Train("alpha beta gamma delta")

	shallowAlpha := childNode(t, childNode(t, shallow.rootNode, "4"), "alpha")
	if len(shallowAlpha.clusterIDs) != 1 {
		t.Fatalf("depth 4 should store cluster at first token node, got ids %#v", shallowAlpha.clusterIDs)
	}
	if _, ok := shallowAlpha.keyToChildNode["beta"]; ok {
		t.Fatal("depth 4 should not index the second token")
	}

	deepConfig := DefaultConfig()
	deepConfig.LogClusterDepth = 6
	deep := New(deepConfig)
	deep.Train("alpha beta gamma delta")

	deepGamma := childNode(t, childNode(t, childNode(t, childNode(t, deep.rootNode, "4"), "alpha"), "beta"), "gamma")
	if len(deepGamma.clusterIDs) != 1 {
		t.Fatalf("depth 6 should store cluster at third token node, got ids %#v", deepGamma.clusterIDs)
	}
}

func TestShortMessagesDoNotDuplicateClustersAtGreaterDepth(t *testing.T) {
	config := DefaultConfig()
	config.LogClusterDepth = 8
	logger := New(config)

	cluster := logger.Train("short line")
	updated := logger.Train("short line")

	if updated != cluster {
		t.Fatalf("expected short message to update cluster %d, got %v", cluster.id, updated)
	}
	if updated.size != 2 {
		t.Fatalf("expected cluster size 2, got %d", updated.size)
	}
	if got := len(logger.Clusters()); got != 1 {
		t.Fatalf("expected one cluster, got %d", got)
	}
}

func TestLogClusterIDReturnsStableID(t *testing.T) {
	logger := New(DefaultConfig())
	trained := logger.Train("user alice logged in")
	if trained.ID() != 1 {
		t.Fatalf("expected trained cluster id 1, got %d", trained.ID())
	}

	restored := New(DefaultConfig())
	if err := restored.LoadClusters([]LogClusterSnapshot{
		{
			ID:             42,
			Size:           3,
			TemplateTokens: []string{"user", "<*>", "logged", "in"},
		},
	}); err != nil {
		t.Fatalf("LoadClusters returned error: %v", err)
	}
	match := restored.Match("user bob logged in")
	if match == nil {
		t.Fatal("expected restored cluster to match")
	}
	if match.ID() != 42 {
		t.Fatalf("expected restored cluster id 42, got %d", match.ID())
	}
}

func tokensForMaskingRules(t *testing.T, line string, rules []MaskingRule) []string {
	t.Helper()
	config := DefaultConfig()
	config.MaskingRules = rules
	logger := New(config)
	return logger.getContentAsTokens(line)
}

func sameTokenBacking(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	return &a[0] == &b[0]
}

func loadTestClusters(t *testing.T, logger *Drain, snapshots []LogClusterSnapshot) {
	t.Helper()
	if err := logger.LoadClusters(snapshots); err != nil {
		t.Fatalf("LoadClusters returned error: %v", err)
	}
}

func childNode(t *testing.T, node *Node, key string) *Node {
	t.Helper()
	child, ok := node.keyToChildNode[key]
	if !ok {
		t.Fatalf("missing child node %q", key)
	}
	return child
}

func assertChildKeys(t *testing.T, node *Node, keys []string) {
	t.Helper()
	if len(node.keyToChildNode) != len(keys) {
		t.Fatalf("child count mismatch: want %d keys %#v, got %d keys %#v", len(keys), keys, len(node.keyToChildNode), mapKeys(node.keyToChildNode))
	}
	for _, key := range keys {
		if _, ok := node.keyToChildNode[key]; !ok {
			t.Fatalf("missing child key %q in %#v", key, mapKeys(node.keyToChildNode))
		}
	}
}

func mapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
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
