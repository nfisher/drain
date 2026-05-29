package drain

import (
	"strconv"
	"strings"
	"testing"

	a "github.com/gogunit/gunit/hammy"
)

const timestampPrefixPattern = `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`

func TestMaskingRuleMasksTimestampPrefix(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{Pattern: timestampPrefixPattern},
	}
	logger := New(config)

	line := "[Mon May 11 13:41:21 2026] Linux version 6.14.0-1008-nvidia-64k (buildd@bos03-arm64-088) (aarch64-linux-gnu-gcc-13 (Ubuntu 13.3.0-6ubuntu2~24.04) 13.3.0, GNU ld (GNU Binutils for Ubuntu) 2.42) #8-Ubuntu SMP PREEMPT_DYNAMIC Sat Jul 26 02:43:53 UTC 2025 (Ubuntu 6.14.0-1008.8-nvidia-64k 6.14.6)"
	cluster := logger.Train(line)

	template := cluster.getTemplate()
	assert.Requires(a.String(template).NotContains("[Mon May 11 13:41:21 2026]"))
	assert.Requires(a.String(template).HasPrefix("<*> Linux version"))

	secondLine := strings.Replace(line, "[Mon May 11 13:41:21 2026]", "[Tue Jun 16 14:42:22 2026]", 1)
	match := logger.Match(secondLine)
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestMaskingRuleUsesLiteralReplacement(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `user-\d+`,
			MaskWith: `$user`,
		},
	}
	logger := New(config)

	cluster := logger.Train("service user-123 ready")

	assert.Requires(a.String(cluster.Template()).EqualTo("service $user ready"))

	match := logger.Match("service user-456 ready")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestMaskingRuleUsesNamedReplacement(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `\b\d{1,3}(?:\.\d{1,3}){3}\b`,
			MaskWith: `IP`,
		},
	}
	logger := New(config)

	cluster := logger.Train("connected to 10.0.0.1")

	assert.Requires(a.String(cluster.Template()).EqualTo("connected to <:IP:>"))

	match := logger.Match("connected to 192.168.0.1")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestMaskingRuleKeepsExplicitLiteralReplacement(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{
			Pattern:  `user-\d+`,
			MaskWith: `<user>`,
		},
	}
	logger := New(config)

	cluster := logger.Train("service user-123 ready")

	assert.Requires(a.String(cluster.Template()).EqualTo("service <user> ready"))
}

func TestExtractParametersReturnsNamedAndEmbeddedRawValues(t *testing.T) {
	assert := a.New(t)
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
	assert.Requires(a.String(cluster.Template()).EqualTo("service id=<:NUM:> path=/users/<:NUM:> status <*>"))

	parameters, ok := logger.ExtractParameters(cluster.Template(), "service id=789 path=/users/99 status retry")
	assert.Requires(a.True(ok))

	assert.Requires(a.Slice(parameters).EqualTo(
		ExtractedParameter{Value: "789", MaskName: "NUM"},
		ExtractedParameter{Value: "99", MaskName: "NUM"},
		ExtractedParameter{Value: "retry", MaskName: "*"},
	))
}

func TestExtractParametersHandlesWhitespaceAndMaskedValuesWithSpaces(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{Pattern: timestampPrefixPattern},
	}
	logger := New(config)

	cluster := logger.Train("[Mon May 11 13:41:21 2026] user alice logged in")
	cluster = logger.Train("[Tue Jun 16 14:42:22 2026] user bob logged in")

	parameters, ok := logger.ExtractParameters(cluster.Template(), "[Wed Jul 17 15:43:23 2026]\t user   carol logged\tin")
	assert.Requires(a.True(ok))

	assert.Requires(a.Slice(parameters).EqualTo(
		ExtractedParameter{Value: "[Wed Jul 17 15:43:23 2026]", MaskName: "*"},
		ExtractedParameter{Value: "carol", MaskName: "*"},
	))
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
			assert := a.New(t)
			fast := tokensForMaskingRules(t, line, fastRules)
			fallback := tokensForMaskingRules(t, line, fallbackRules)
			assert.Requires(a.Slice(fast).EqualTo(fallback...))
		})
	}
}

func TestContentTokenizationUsesDrain3Whitespace(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())

	got := logger.getContentAsTokens(" \talpha  beta\tgamma\n")
	assert.Requires(a.Slice(got).EqualTo("alpha", "beta", "gamma"))

	assert.Requires(a.Number(len(logger.getContentAsTokens(" \t "))).EqualTo(0))
}

func TestContentTokenizationUsesExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_", ":"}
	logger := New(config)

	got := logger.getContentAsTokens(" \talpha__beta:gamma  delta::epsilon\n")
	assert.Requires(a.Slice(got).EqualTo("alpha", "beta", "gamma", "delta", "epsilon"))
}

func TestExtraDelimitersApplyAfterMasking(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{":"}
	config.MaskingRules = []MaskingRule{{Pattern: timestampPrefixPattern}}
	logger := New(config)

	got := logger.getContentAsTokens("[Mon May 11 13:41:21 2026]:user:alice")
	assert.Requires(a.Slice(got).EqualTo("<*>", "user", "alice"))
}

func TestExtraDelimitersDoNotSplitNamedMasks(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{":"}
	config.MaskingRules = []MaskingRule{
		{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, MaskWith: "IP"},
	}
	logger := New(config)

	got := logger.getContentAsTokens("addr:192.0.2.10")
	assert.Requires(a.Slice(got).EqualTo("addr", "<:IP:>"))
}

func TestMaskingRulesDoNotCascadeIntoEarlierReplacements(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaskingRules = []MaskingRule{
		{Pattern: `\bv?\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\b`, MaskWith: "VERSION"},
		{Pattern: `(?:/[^\s:/]+)+`, MaskWith: "PATH"},
	}
	logger := New(config)

	line := "endpoint=http://192.0.2.10:9000"
	cluster := logger.Train(line)
	assert.Requires(a.String(cluster.Template()).EqualTo("endpoint=http://<:VERSION:>.10:9000"))

	assert.Requires(a.String(cluster.Template()).NotContains("<:PATH:>:VERSION:>"))

	parameters, ok := logger.ExtractParameters(cluster.Template(), line)
	assert.Requires(a.True(ok))

	assert.Requires(a.Slice(parameters).EqualTo(ExtractedParameter{Value: "192.0.2", MaskName: "VERSION"}))
}

func TestNewRejectsEmptyExtraDelimiter(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_", ""}

	defer func() {
		recovered := recover()
		message, _ := recovered.(string)
		assert.Requires(a.NotNil(recovered))
		assert.Requires(a.String(message).Contains("extra delimiter must not be empty"))
	}()
	New(config)
}

func TestTrainSimilarityIgnoresWhitespaceRuns(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	cluster := logger.Train("user  alice\tlogged in")

	updated := logger.Train("user bob logged in")
	assert.Requires(a.Match(updated, a.SamePointer(cluster)))
	assert.Requires(a.String(updated.Template()).EqualTo("user <*> logged in"))
}

func TestTrainAndMatchUseExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{"_"}
	logger := New(config)

	cluster := logger.Train("user_alice_logged_in")
	updated := logger.Train("user_bob_logged_in")
	assert.Requires(a.Match(updated, a.SamePointer(cluster)))
	assert.Requires(a.String(updated.Template()).EqualTo("user <*> logged in"))

	match := logger.Match("user_carol_logged_in")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestExtractParametersPreservesMaskedDelimiterValues(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.ExtraDelimiters = []string{":"}
	config.MaskingRules = []MaskingRule{{Pattern: timestampPrefixPattern}}
	logger := New(config)
	cluster := logger.Train("[Mon May 11 13:41:21 2026]:user:alice:logged:in")
	cluster = logger.Train("[Tue Jun 16 14:42:22 2026]:user:bob:logged:in")

	parameters, ok := logger.ExtractParameters(cluster.Template(), "[Wed Jul 17 15:43:23 2026]:user:carol:logged:in")
	assert.Requires(a.True(ok))

	assert.Requires(a.Slice(parameters).EqualTo(
		ExtractedParameter{Value: "[Wed Jul 17 15:43:23 2026]", MaskName: "*"},
		ExtractedParameter{Value: "carol", MaskName: "*"},
	))
}

func TestBlankInputProducesZeroTokenCluster(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())

	cluster := logger.Train(" \t ")
	assert.Requires(a.Number(len(cluster.logTemplateTokens)).EqualTo(0))
	match := logger.Match("\t  ")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestMatchKeepsTreeOnlySearchByDefault(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "fixed", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "target", "ready"}},
	})

	assert.Requires(a.Nil(logger.Match("alpha target ready")))
}

func TestMatchWithOptionsFallbackFindsWildcardBranch(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "fixed", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "target", "ready"}},
	})

	match := logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchFallback,
	})
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(2))
}

func TestMatchWithOptionsAlwaysSearchesAllSameLengthClusters(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	loadTestClusters(t, logger, []LogClusterSnapshot{
		{ID: 1, Size: 1, TemplateTokens: []string{"alpha", "target", "ready"}},
		{ID: 2, Size: 1, TemplateTokens: []string{"<*>", "<*>", "ready"}},
	})
	{
		match := logger.MatchWithOptions("alpha target ready", MatchOptions{
			FullSearchStrategy: FullSearchFallback,
		})
		assert.Requires(a.NotNil(match))
		assert.Requires(a.Number(match.id).EqualTo(1))
	}

	match := logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchAlways,
	})
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(2))
}

func TestMatchWithOptionsRejectsInvalidFullSearchStrategy(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	logger.Train("alpha target ready")

	defer func() {
		assert.Requires(a.NotNil(recover()))
	}()
	logger.MatchWithOptions("alpha target ready", MatchOptions{
		FullSearchStrategy: FullSearchStrategy("sometimes"),
	})
}

func TestMatchWithOptionsFullSearchHandlesBlankInput(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	cluster := logger.Train(" \t ")

	match := logger.MatchWithOptions(" ", MatchOptions{
		FullSearchStrategy: FullSearchAlways,
	})
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestMatchDoesNotRefreshClusterCacheRecency(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaxClusters = 2
	logger := New(config)

	first := logger.Train("alpha one")
	logger.Train("beta two")
	match := logger.Match("alpha one")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(first.id))

	logger.Train("gamma three")

	assert.Requires(a.Nil(logger.Match("alpha one")))

	assert.Requires(a.NotNil(logger.Match("beta two")))
}

func TestTrainRefreshesAcceptedClusterCacheRecency(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.MaxClusters = 2
	logger := New(config)

	first := logger.Train("alpha one")
	logger.Train("beta two")

	assert.Requires(a.Match(logger.Train("alpha one"), a.SamePointer(first)))

	logger.Train("gamma three")
	match := logger.Match("alpha one")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(first.id))

	assert.Requires(a.Nil(logger.Match("beta two")))
}

func TestTrainKeepsTemplateTokensWhenTemplateIsUnchanged(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	cluster := logger.Train("fixed line")
	before := cluster.logTemplateTokens

	updated := logger.Train("fixed line")
	assert.Requires(a.Match(updated, a.SamePointer(cluster)))
	assert.Requires(a.Number(updated.size).EqualTo(2))
	assert.Requires(a.True(sameTokenBacking(before, updated.logTemplateTokens)))
	assert.Requires(a.String(updated.Template()).EqualTo("fixed line"))
}

func TestTrainKeepsTemplateTokensWhenAlreadyGeneralized(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	cluster := logger.Train("user alice logged in")
	cluster = logger.Train("user bob logged in")

	assert.Requires(a.String(cluster.Template()).EqualTo("user <*> logged in"))

	before := cluster.logTemplateTokens

	updated := logger.Train("user carol logged in")
	assert.Requires(a.Match(updated, a.SamePointer(cluster)))
	assert.Requires(a.Number(updated.size).EqualTo(3))
	assert.Requires(a.True(sameTokenBacking(before, updated.logTemplateTokens)))
	assert.Requires(a.String(updated.Template()).EqualTo("user <*> logged in"))

	match := logger.Match("user dave logged in")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(cluster.id))
}

func TestNumericTokensAreParameterizedByDefault(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.LogClusterDepth = 5
	logger := New(config)

	first := logger.Train("host web-001 ready")
	second := logger.Train("host web-002 ready")
	assert.Requires(a.Match(second, a.SamePointer(first)))
	assert.Requires(a.String(second.Template()).EqualTo("host <*> ready"))
}

func TestPreserveNumericTokensKeepsNumericPrefixesExact(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.LogClusterDepth = 5
	config.PreserveNumericTokens = true
	logger := New(config)

	first := logger.Train("host web-001 ready")
	second := logger.Train("host web-002 ready")
	assert.Requires(a.Match(second, a.Not(a.SamePointer(first))))

	assert.Requires(a.Number(len(logger.Clusters())).EqualTo(2))
}

func TestMaxChildrenSpillsDistinctBranchesToWildcardAtLimit(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.LogClusterDepth = 5
	config.MaxChildren = 3
	logger := New(config)

	logger.Train("alpha common tail")
	logger.Train("beta common tail")
	spillCluster := logger.Train("gamma common tail")
	updatedSpillCluster := logger.Train("delta common tail")
	assert.Requires(a.Match(updatedSpillCluster, a.SamePointer(spillCluster)))
	assert.Requires(a.String(updatedSpillCluster.Template()).EqualTo("<*> common tail"))

	tokenCountNode := childNode(t, logger.rootNode, "3")
	assertChildKeys(t, tokenCountNode, []string{"<*>", "alpha", "beta"})
	_, ok := tokenCountNode.keyToChildNode["gamma"]
	assert.Requires(a.False(ok))
	_, ok = tokenCountNode.keyToChildNode["delta"]
	assert.Requires(a.False(ok))
}

func TestMaxChildrenCountsExistingWildcardChild(t *testing.T) {
	assert := a.New(t)
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
	_, ok := tokenCountNode.keyToChildNode["gamma"]
	assert.Requires(a.False(ok))
}

func TestNewRejectsNonPositiveMaxChildren(t *testing.T) {
	for _, maxChildren := range []int{0, -1} {
		t.Run(strconv.Itoa(maxChildren), func(t *testing.T) {
			assert := a.New(t)
			config := DefaultConfig()
			config.MaxChildren = maxChildren

			defer func() {
				recovered := recover()
				message, _ := recovered.(string)
				assert.Requires(a.NotNil(recovered))
				assert.Requires(a.String(message).Contains("max children must be at least 1"))
			}()
			New(config)
		})
	}
}

func TestLogClusterDepthBoundsPrefixTree(t *testing.T) {
	assert := a.New(t)
	shallowConfig := DefaultConfig()
	shallowConfig.LogClusterDepth = 4
	shallow := New(shallowConfig)
	shallow.Train("alpha beta gamma delta")

	shallowAlpha := childNode(t, childNode(t, shallow.rootNode, "4"), "alpha")
	assert.Requires(a.Number(len(shallowAlpha.clusterIDs)).EqualTo(1))
	_, ok := shallowAlpha.keyToChildNode["beta"]
	assert.Requires(a.False(ok))

	deepConfig := DefaultConfig()
	deepConfig.LogClusterDepth = 6
	deep := New(deepConfig)
	deep.Train("alpha beta gamma delta")

	deepGamma := childNode(t, childNode(t, childNode(t, childNode(t, deep.rootNode, "4"), "alpha"), "beta"), "gamma")
	assert.Requires(a.Number(len(deepGamma.clusterIDs)).EqualTo(1))
}

func TestShortMessagesDoNotDuplicateClustersAtGreaterDepth(t *testing.T) {
	assert := a.New(t)
	config := DefaultConfig()
	config.LogClusterDepth = 8
	logger := New(config)

	cluster := logger.Train("short line")
	updated := logger.Train("short line")
	assert.Requires(a.Match(updated, a.SamePointer(cluster)))
	assert.Requires(a.Number(updated.size).EqualTo(2))

	assert.Requires(a.Number(len(logger.Clusters())).EqualTo(1))
}

func TestLogClusterIDReturnsStableID(t *testing.T) {
	assert := a.New(t)
	logger := New(DefaultConfig())
	trained := logger.Train("user alice logged in")
	assert.Requires(a.Number(trained.ID()).EqualTo(1))

	restored := New(DefaultConfig())

	assert.Requires(a.NilError(restored.LoadClusters([]LogClusterSnapshot{
		{
			ID:             42,
			Size:           3,
			TemplateTokens: []string{"user", "<*>", "logged", "in"},
		},
	}),
	))

	match := restored.Match("user bob logged in")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.ID()).EqualTo(42))
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
	assert := a.New(t)

	assert.Requires(a.NilError(logger.LoadClusters(snapshots)))
}

func childNode(t *testing.T, node *Node, key string) *Node {
	t.Helper()
	assert := a.New(t)
	child, ok := node.keyToChildNode[key]
	assert.Requires(a.True(ok))

	return child
}

func assertChildKeys(t *testing.T, node *Node, keys []string) {
	t.Helper()
	assert := a.New(t)
	assert.Requires(a.Number(len(node.keyToChildNode)).EqualTo(len(keys)))

	for _, key := range keys {
		_, ok := node.keyToChildNode[key]
		assert.Requires(a.True(ok))
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
	assert := a.New(t)
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

	assert.Requires(a.NilError(logger.LoadClusters(inputSnapshots)))

	inputSnapshots[0].TemplateTokens[1] = "mutated"

	assert.Requires(a.Nil(logger.Match("old cluster line")))

	match := logger.Match("user alice logged in")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(7))
	assert.Requires(a.Number(match.size).EqualTo(2))

	snapshots := logger.ClusterSnapshots()
	assert.Requires(a.Number(len(snapshots)).EqualTo(1))
	assert.Requires(a.Assert(!(snapshots[0].ID != 7 || snapshots[0].Size != 2), "snapshot did not preserve id/size: %+v", snapshots[0]))
	assert.Requires(a.String(snapshots[0].TemplateTokens[1]).EqualTo(config.ParamString))

	snapshots[0].TemplateTokens[1] = "mutated again"
	match = logger.Match("user bob logged in")
	assert.Requires(a.NotNil(match))
	assert.Requires(a.Number(match.id).EqualTo(7))

	updated := logger.Train("user carol logged in")
	assert.Requires(a.Number(updated.id).EqualTo(7))
	assert.Requires(a.Number(updated.size).EqualTo(3))

	created := logger.Train("connected to 10.0.0.1")
	assert.Requires(a.Number(created.id).EqualTo(8))
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
			assert := a.New(t)
			config := DefaultConfig()
			if tt.configure != nil {
				tt.configure(config)
			}
			logger := New(config)
			logger.Train("kept line")

			assert.Requires(a.Error(logger.LoadClusters(tt.snapshots)))

			assert.Requires(a.NotNil(logger.Match("kept line")))
		})
	}
}
