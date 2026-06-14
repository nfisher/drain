package drain

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type Config struct {
	maxNodeDepth    int
	LogClusterDepth int
	SimTh           float64
	// MaxChildren caps child nodes per internal prefix-tree node. The wildcard
	// ParamString child counts toward this cap.
	MaxChildren     int
	ExtraDelimiters []string
	MaxClusters     int
	ParamString     string
	// PreserveNumericTokens keeps tokens containing digits as exact prefix-tree
	// keys instead of routing them through ParamString.
	PreserveNumericTokens bool
	// MaskingRules replace known variable patterns before tokenization.
	MaskingRules []MaskingRule
}

// MaskingRule describes a regex replacement applied before Drain tokenization.
type MaskingRule struct {
	Pattern string
	// MaskWith names a Drain3-style mask. When empty, Config.ParamString is used.
	// Values containing '<', '>', or '$' are kept as explicit literal replacements.
	MaskWith string
	// Replacement, when set, is the exact token inserted by this rule. It is
	// useful when loading models that were trained before named-mask rendering.
	Replacement string
}

// ExtractedParameter is a variable value extracted from a log line. MaskName is
// the Drain3 mask name, or "*" for the catch-all ParamString parameter.
type ExtractedParameter struct {
	Value    string `json:"value"`
	MaskName string `json:"mask_name"`
}

// FullSearchStrategy controls whether MatchWithOptions searches beyond the
// prefix-tree candidate node.
type FullSearchStrategy string

const (
	// FullSearchNever only uses the prefix tree. It is the fastest strategy, but
	// can miss a matching wildcard template in another branch.
	FullSearchNever FullSearchStrategy = "never"
	// FullSearchFallback uses the prefix tree first and scans same-length
	// clusters only when tree search misses.
	FullSearchFallback FullSearchStrategy = "fallback"
	// FullSearchAlways always scans all same-length clusters.
	FullSearchAlways FullSearchStrategy = "always"
)

// MatchOptions configures MatchWithOptions.
type MatchOptions struct {
	FullSearchStrategy FullSearchStrategy
}

type LogCluster struct {
	mu                sync.RWMutex
	logTemplateTokens []string
	id                int
	size              int
}

// LogClusterSnapshot is a serializable representation of a Drain cluster.
type LogClusterSnapshot struct {
	ID             int
	Size           int
	TemplateTokens []string
}

func (c *LogCluster) getTemplate() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTemplateLocked()
}

func (c *LogCluster) getTemplateLocked() string {
	return strings.Join(c.logTemplateTokens, " ")
}

// Template returns the cluster template as a space-joined string.
func (c *LogCluster) Template() string {
	return c.getTemplate()
}

func (c *LogCluster) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Sprintf("id={%d} : size={%d} : %s", c.id, c.size, c.getTemplateLocked())
}

// ID returns the stable cluster identifier.
func (c *LogCluster) ID() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

// Snapshot returns a copy of the cluster state.
func (c *LogCluster) Snapshot() LogClusterSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tokens := make([]string, len(c.logTemplateTokens))
	copy(tokens, c.logTemplateTokens)
	return LogClusterSnapshot{
		ID:             c.id,
		Size:           c.size,
		TemplateTokens: tokens,
	}
}

func createLogClusterCache(maxSize int) *LogClusterCache {
	if maxSize == 0 {
		maxSize = math.MaxInt
	}
	cache, _ := simplelru.NewLRU[int, *LogCluster](maxSize, nil)
	return &LogClusterCache{
		cache: cache,
	}
}

type LogClusterCache struct {
	cache simplelru.LRUCache[int, *LogCluster]
}

func (c *LogClusterCache) Values() []*LogCluster {
	values := make([]*LogCluster, 0)
	for _, key := range c.cache.Keys() {
		if value, ok := c.cache.Peek(key); ok {
			values = append(values, value)
		}
	}
	return values
}

func (c *LogClusterCache) Set(key int, cluster *LogCluster) {
	c.cache.Add(key, cluster)
}

func (c *LogClusterCache) Get(key int) *LogCluster {
	cluster, ok := c.cache.Get(key)
	if !ok {
		return nil
	}
	return cluster
}

func (c *LogClusterCache) Peek(key int) *LogCluster {
	cluster, ok := c.cache.Peek(key)
	if !ok {
		return nil
	}
	return cluster
}

func createNode() *Node {
	return &Node{
		keyToChildNode: make(map[string]*Node),
		clusterIDs:     make([]int, 0),
	}
}

type Node struct {
	keyToChildNode map[string]*Node
	clusterIDs     []int
}

func DefaultConfig() *Config {
	return &Config{
		LogClusterDepth: 4,
		SimTh:           0.4,
		MaxChildren:     100,
		ParamString:     "<*>",
	}
}

func New(config *Config) *Drain {
	if config.LogClusterDepth < 3 {
		panic("depth argument must be at least 3")
	}
	if config.MaxChildren < 1 {
		panic("max children must be at least 1")
	}
	validateExtraDelimiters(config.ExtraDelimiters)
	config.maxNodeDepth = config.LogClusterDepth - 2

	d := &Drain{
		config:                        config,
		rootNode:                      createNode(),
		idToCluster:                   createLogClusterCache(config.MaxClusters),
		maskingRules:                  compileMaskingRules(config.MaskingRules, config.ParamString),
		parameterExtractionRegexCache: make(map[string]compiledParameterExtraction),
	}
	return d
}

func validateExtraDelimiters(extraDelimiters []string) {
	for _, extraDelimiter := range extraDelimiters {
		if extraDelimiter == "" {
			panic("extra delimiter must not be empty")
		}
	}
}

type Drain struct {
	mu                            sync.RWMutex
	config                        *Config
	rootNode                      *Node
	idToCluster                   *LogClusterCache
	clustersCounter               int
	maskingRules                  []compiledMaskingRule
	parameterExtractionRegexCache map[string]compiledParameterExtraction
}

type compiledMaskingRule struct {
	pattern     string
	regex       *regexp.Regexp
	maskWith    string
	maskName    string
	replacement string
}

type compiledParameterExtraction struct {
	regex          *regexp.Regexp
	groupMaskNames map[string]string
}

const (
	drain3MaskPrefix = "<:"
	drain3MaskSuffix = ":>"
	catchAllMaskName = "*"
)

func (d *Drain) Clusters() []*LogCluster {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.idToCluster.Values()
}

// ClusterSnapshots returns a stable, ID-sorted copy of all cluster states.
func (d *Drain) ClusterSnapshots() []LogClusterSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	clusters := d.idToCluster.Values()
	snapshots := make([]LogClusterSnapshot, 0, len(clusters))
	for _, cluster := range clusters {
		snapshots = append(snapshots, cluster.Snapshot())
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].ID < snapshots[j].ID
	})
	return snapshots
}

// LoadClusters replaces the current cluster state and rebuilds the prefix tree.
func (d *Drain) LoadClusters(snapshots []LogClusterSnapshot) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.config.MaxClusters > 0 && len(snapshots) > d.config.MaxClusters {
		return fmt.Errorf("snapshot contains %d clusters, max clusters is %d", len(snapshots), d.config.MaxClusters)
	}

	seenIDs := make(map[int]struct{}, len(snapshots))
	clusters := make([]*LogCluster, 0, len(snapshots))
	maxID := 0
	for _, snapshot := range snapshots {
		if snapshot.ID <= 0 {
			return fmt.Errorf("cluster id must be positive, got %d", snapshot.ID)
		}
		if snapshot.Size <= 0 {
			return fmt.Errorf("cluster %d size must be positive, got %d", snapshot.ID, snapshot.Size)
		}
		if _, ok := seenIDs[snapshot.ID]; ok {
			return fmt.Errorf("duplicate cluster id %d", snapshot.ID)
		}
		seenIDs[snapshot.ID] = struct{}{}

		tokens := make([]string, len(snapshot.TemplateTokens))
		copy(tokens, snapshot.TemplateTokens)
		clusters = append(clusters, &LogCluster{
			logTemplateTokens: tokens,
			id:                snapshot.ID,
			size:              snapshot.Size,
		})
		if snapshot.ID > maxID {
			maxID = snapshot.ID
		}
	}

	d.rootNode = createNode()
	d.idToCluster = createLogClusterCache(d.config.MaxClusters)
	d.clustersCounter = maxID
	for _, cluster := range clusters {
		d.idToCluster.Set(cluster.id, cluster)
		d.addSeqToPrefixTree(d.rootNode, cluster)
	}
	return nil
}

func (d *Drain) Train(content string) *LogCluster {
	d.mu.Lock()
	defer d.mu.Unlock()
	contentTokens := d.getContentAsTokens(content)

	matchCluster := d.treeSearch(d.rootNode, contentTokens, d.config.SimTh, false)
	// Match no existing log cluster
	if matchCluster == nil {
		d.clustersCounter++
		clusterID := d.clustersCounter
		matchCluster = &LogCluster{
			logTemplateTokens: contentTokens,
			id:                clusterID,
			size:              1,
		}
		d.idToCluster.Set(clusterID, matchCluster)
		d.addSeqToPrefixTree(d.rootNode, matchCluster)
	} else {
		matchCluster.mu.Lock()
		newTemplateTokens := d.createTemplate(contentTokens, matchCluster.logTemplateTokens)
		matchCluster.logTemplateTokens = newTemplateTokens
		matchCluster.size++
		matchCluster.mu.Unlock()
		// Touch cluster to update its state in the cache.
		d.idToCluster.Get(matchCluster.id)
	}
	return matchCluster
}

// Match against an already existing cluster. Match shall be perfect (sim_th=1.0). New cluster will not be created as a result of this call, nor any cluster modifications.
func (d *Drain) Match(content string) *LogCluster {
	return d.MatchWithOptions(content, MatchOptions{})
}

// MatchWithOptions matches against existing clusters without creating or
// modifying clusters. Match shall be perfect (sim_th=1.0).
func (d *Drain) MatchWithOptions(content string, options MatchOptions) *LogCluster {
	d.mu.RLock()
	defer d.mu.RUnlock()
	contentTokens := d.getContentAsTokens(content)
	strategy := options.FullSearchStrategy
	if strategy == "" {
		strategy = FullSearchNever
	}

	fullSearch := func() *LogCluster {
		clusterIDs := d.getClusterIDsForTokenCount(len(contentTokens))
		return d.fastMatch(clusterIDs, contentTokens, 1.0, true)
	}

	switch strategy {
	case FullSearchAlways:
		return fullSearch()
	case FullSearchNever, FullSearchFallback:
		matchCluster := d.treeSearch(d.rootNode, contentTokens, 1.0, true)
		if matchCluster != nil || strategy == FullSearchNever {
			return matchCluster
		}
		return fullSearch()
	default:
		panic(fmt.Sprintf("invalid full search strategy %q", strategy))
	}
}

func (d *Drain) getContentAsTokens(content string) []string {
	content = strings.TrimSpace(content)
	if len(d.maskingRules) == 0 {
		content = replaceExtraDelimiters(content, d.config.ExtraDelimiters)
		return strings.Fields(content)
	}

	var masked strings.Builder
	for _, segment := range d.maskContentSegments(content) {
		if segment.masked {
			masked.WriteString(segment.value)
			continue
		}
		masked.WriteString(replaceExtraDelimiters(segment.rawString, d.config.ExtraDelimiters))
	}
	return strings.Fields(masked.String())
}

func replaceExtraDelimiters(content string, extraDelimiters []string) string {
	for _, extraDelimiter := range extraDelimiters {
		content = strings.Replace(content, extraDelimiter, " ", -1)
	}
	return content
}

func compileMaskingRules(rules []MaskingRule, defaultReplacement string) []compiledMaskingRule {
	compiled := make([]compiledMaskingRule, 0, len(rules))
	for _, rule := range rules {
		if rule.Pattern == "" {
			panic("masking rule pattern must not be empty")
		}
		regex, err := regexp.Compile(rule.Pattern)
		if err != nil {
			panic(fmt.Sprintf("invalid masking rule pattern %q: %v", rule.Pattern, err))
		}
		replacement, maskName := maskingRuleReplacement(rule, defaultReplacement)
		compiled = append(compiled, compiledMaskingRule{
			pattern:     rule.Pattern,
			regex:       regex,
			maskWith:    rule.MaskWith,
			maskName:    maskName,
			replacement: replacement,
		})
	}
	return compiled
}

func maskingRuleReplacement(rule MaskingRule, defaultReplacement string) (string, string) {
	if rule.Replacement != "" {
		return rule.Replacement, maskNameForReplacement(rule.Replacement, defaultReplacement)
	}
	if rule.MaskWith == "" {
		return defaultReplacement, catchAllMaskName
	}
	if isExplicitMaskReplacement(rule.MaskWith) {
		return rule.MaskWith, maskNameForReplacement(rule.MaskWith, defaultReplacement)
	}
	return namedMaskToken(rule.MaskWith), rule.MaskWith
}

func isExplicitMaskReplacement(maskWith string) bool {
	return strings.ContainsAny(maskWith, "<>$")
}

func namedMaskToken(maskName string) string {
	return drain3MaskPrefix + maskName + drain3MaskSuffix
}

func maskNameForReplacement(replacement, paramString string) string {
	if replacement == paramString {
		return catchAllMaskName
	}
	if maskName, ok := namedMaskName(replacement); ok {
		return maskName
	}
	return ""
}

func namedMaskName(token string) (string, bool) {
	if !strings.HasPrefix(token, drain3MaskPrefix) || !strings.HasSuffix(token, drain3MaskSuffix) {
		return "", false
	}
	maskName := token[len(drain3MaskPrefix) : len(token)-len(drain3MaskSuffix)]
	if maskName == "" {
		return "", false
	}
	return maskName, true
}

// ExtractParameters matches content against logTemplate and returns the ordered
// template parameters. It recognizes Config.ParamString and Drain3-style named
// mask tokens such as <:IP:>.
func (d *Drain) ExtractParameters(logTemplate, content string) ([]ExtractedParameter, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	compiled, ok := d.parameterExtractionRegexCache[logTemplate]
	if !ok {
		var err error
		compiled, err = d.compileParameterExtraction(logTemplate)
		if err != nil {
			return nil, false
		}
		d.parameterExtractionRegexCache[logTemplate] = compiled
	}

	matches := compiled.regex.FindStringSubmatch(d.prepareContentForExtraction(content))
	if matches == nil {
		return nil, false
	}

	parameters := make([]ExtractedParameter, 0, len(compiled.groupMaskNames))
	for i, groupName := range compiled.regex.SubexpNames() {
		maskName, ok := compiled.groupMaskNames[groupName]
		if !ok {
			continue
		}
		parameters = append(parameters, ExtractedParameter{
			Value:    matches[i],
			MaskName: maskName,
		})
	}
	return parameters, true
}

func (d *Drain) compileParameterExtraction(logTemplate string) (compiledParameterExtraction, error) {
	pattern, groupMaskNames := d.parameterExtractionRegex(logTemplate)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return compiledParameterExtraction{}, err
	}
	return compiledParameterExtraction{
		regex:          regex,
		groupMaskNames: groupMaskNames,
	}, nil
}

func (d *Drain) prepareContentForExtraction(content string) string {
	content = strings.TrimSpace(content)
	if len(d.maskingRules) == 0 {
		return replaceExtraDelimiters(content, d.config.ExtraDelimiters)
	}

	var prepared strings.Builder
	for _, segment := range d.maskContentSegments(content) {
		if segment.masked {
			prepared.WriteString(segment.rawString)
			continue
		}
		prepared.WriteString(replaceExtraDelimiters(segment.rawString, d.config.ExtraDelimiters))
	}
	return prepared.String()
}

type contentSegment struct {
	masked    bool
	value     string
	rawString string
}

func (d *Drain) maskContentSegments(content string) []contentSegment {
	segments := []contentSegment{{rawString: content}}
	for _, rule := range d.maskingRules {
		next := make([]contentSegment, 0, len(segments))
		for _, segment := range segments {
			if segment.masked {
				next = append(next, segment)
				continue
			}
			next = append(next, maskContentSegment(segment.rawString, rule)...)
		}
		segments = next
	}
	return segments
}

func maskContentSegment(content string, rule compiledMaskingRule) []contentSegment {
	matches := rule.regex.FindAllStringIndex(content, -1)
	if len(matches) == 0 {
		return []contentSegment{{rawString: content}}
	}

	segments := make([]contentSegment, 0, len(matches)*2+1)
	offset := 0
	for _, match := range matches {
		if match[0] > offset {
			segments = append(segments, contentSegment{rawString: content[offset:match[0]]})
		}
		segments = append(segments, contentSegment{
			masked:    true,
			value:     rule.replacement,
			rawString: content[match[0]:match[1]],
		})
		offset = match[1]
	}
	if offset < len(content) {
		segments = append(segments, contentSegment{rawString: content[offset:]})
	}
	return segments
}

func (d *Drain) parameterExtractionRegex(logTemplate string) (string, map[string]string) {
	var pattern strings.Builder
	groupMaskNames := make(map[string]string)
	paramCount := 0
	offset := 0
	for {
		param, ok := nextTemplateParameter(logTemplate, offset, d.config.ParamString)
		if !ok {
			appendQuotedFlexibleWhitespace(&pattern, logTemplate[offset:])
			break
		}
		appendQuotedFlexibleWhitespace(&pattern, logTemplate[offset:param.start])
		groupName := fmt.Sprintf("drain_param_%d", paramCount)
		paramCount++
		groupMaskNames[groupName] = param.maskName
		pattern.WriteString("(?P<")
		pattern.WriteString(groupName)
		pattern.WriteString(">")
		pattern.WriteString(d.captureRegexForMask(param.maskName))
		pattern.WriteString(")")
		offset = param.end
	}
	return "^" + pattern.String() + "$", groupMaskNames
}

type templateParameter struct {
	start    int
	end      int
	maskName string
}

func nextTemplateParameter(template string, offset int, paramString string) (templateParameter, bool) {
	best := templateParameter{start: -1}
	if paramString != "" {
		if index := strings.Index(template[offset:], paramString); index >= 0 {
			start := offset + index
			best = templateParameter{
				start:    start,
				end:      start + len(paramString),
				maskName: catchAllMaskName,
			}
		}
	}

	for searchOffset := offset; searchOffset < len(template); {
		index := strings.Index(template[searchOffset:], drain3MaskPrefix)
		if index < 0 {
			break
		}
		start := searchOffset + index
		nameStart := start + len(drain3MaskPrefix)
		suffixIndex := strings.Index(template[nameStart:], drain3MaskSuffix)
		if suffixIndex < 0 {
			break
		}
		end := nameStart + suffixIndex + len(drain3MaskSuffix)
		maskName := template[nameStart : nameStart+suffixIndex]
		if maskName != "" && (best.start < 0 || start < best.start) {
			best = templateParameter{
				start:    start,
				end:      end,
				maskName: maskName,
			}
		}
		break
	}

	if best.start < 0 {
		return templateParameter{}, false
	}
	return best, true
}

func appendQuotedFlexibleWhitespace(pattern *strings.Builder, literal string) {
	literalStart := -1
	inWhitespace := false
	for index, r := range literal {
		if unicode.IsSpace(r) {
			if literalStart >= 0 {
				pattern.WriteString(regexp.QuoteMeta(literal[literalStart:index]))
				literalStart = -1
			}
			if !inWhitespace {
				pattern.WriteString(`\s+`)
				inWhitespace = true
			}
			continue
		}
		if literalStart < 0 {
			literalStart = index
		}
		inWhitespace = false
	}
	if literalStart >= 0 {
		pattern.WriteString(regexp.QuoteMeta(literal[literalStart:]))
	}
}

func (d *Drain) captureRegexForMask(maskName string) string {
	if maskName == catchAllMaskName {
		return ".+?"
	}
	patterns := make([]string, 0)
	for _, rule := range d.maskingRules {
		if rule.maskName == maskName {
			patterns = append(patterns, "(?:"+rule.pattern+")")
		}
	}
	if len(patterns) == 0 {
		return ".+?"
	}
	return strings.Join(patterns, "|")
}

func (d *Drain) treeSearch(rootNode *Node, tokens []string, simTh float64, includeParams bool) *LogCluster {
	tokenCount := len(tokens)

	// at first level, children are grouped by token (word) count
	curNode, ok := rootNode.keyToChildNode[strconv.Itoa(tokenCount)]

	// no template with same token count yet
	if !ok {
		return nil
	}

	// handle case of empty log string - return the single cluster in that group
	if tokenCount == 0 {
		return d.idToCluster.Peek(curNode.clusterIDs[0])
	}

	// find the leaf node for this log - a path of nodes matching the first N tokens (N=tree depth)
	curNodeDepth := 1
	for _, token := range tokens {
		// at max depth
		if curNodeDepth >= d.config.maxNodeDepth {
			break
		}

		// this is last token
		if curNodeDepth == tokenCount {
			break
		}

		keyToChildNode := curNode.keyToChildNode
		curNode, ok = keyToChildNode[token]
		if !ok { // no exact next token exist, try wildcard node
			curNode, ok = keyToChildNode[d.config.ParamString]
		}
		if !ok { // no wildcard node exist
			return nil
		}
		curNodeDepth++
	}

	// get best match among all clusters with same prefix, or None if no match is above sim_th
	cluster := d.fastMatch(curNode.clusterIDs, tokens, simTh, includeParams)
	return cluster
}

func (d *Drain) getClusterIDsForTokenCount(tokenCount int) []int {
	curNode, ok := d.rootNode.keyToChildNode[strconv.Itoa(tokenCount)]
	if !ok {
		return nil
	}
	clusterIDs := make([]int, 0)
	appendClusterIDsRecursive(curNode, &clusterIDs)
	return clusterIDs
}

func appendClusterIDsRecursive(node *Node, clusterIDs *[]int) {
	*clusterIDs = append(*clusterIDs, node.clusterIDs...)
	for _, childNode := range node.keyToChildNode {
		appendClusterIDsRecursive(childNode, clusterIDs)
	}
}

// fastMatch Find the best match for a log message (represented as tokens) versus a list of clusters
func (d *Drain) fastMatch(clusterIDs []int, tokens []string, simTh float64, includeParams bool) *LogCluster {
	var matchCluster, maxCluster *LogCluster

	maxSim := -1.0
	maxParamCount := -1
	for _, clusterID := range clusterIDs {
		// Try to retrieve cluster from cache with bypassing eviction
		// algorithm as we are only testing candidates for a match.
		cluster := d.idToCluster.Peek(clusterID)
		if cluster == nil {
			continue
		}
		cluster.mu.RLock()
		curSim, paramCount := d.getSeqDistance(cluster.logTemplateTokens, tokens, includeParams)
		cluster.mu.RUnlock()
		if curSim > maxSim || (curSim == maxSim && paramCount > maxParamCount) {
			maxSim = curSim
			maxParamCount = paramCount
			maxCluster = cluster
		}
	}
	if maxSim >= simTh {
		matchCluster = maxCluster
	}
	return matchCluster
}

func (d *Drain) getSeqDistance(seq1, seq2 []string, includeParams bool) (float64, int) {
	if len(seq1) != len(seq2) {
		panic("seq1 seq2 be of same length")
	}
	if len(seq1) == 0 {
		return 1.0, 0
	}

	simTokens := 0
	paramCount := 0
	for i := range seq1 {
		token1 := seq1[i]
		token2 := seq2[i]
		if token1 == d.config.ParamString {
			paramCount++
		} else if token1 == token2 {
			simTokens++
		}
	}
	if includeParams {
		simTokens += paramCount
	}
	retVal := float64(simTokens) / float64(len(seq1))
	return retVal, paramCount
}

func (d *Drain) addSeqToPrefixTree(rootNode *Node, cluster *LogCluster) {
	tokenCount := len(cluster.logTemplateTokens)
	tokenCountStr := strconv.Itoa(tokenCount)

	firstLayerNode, ok := rootNode.keyToChildNode[tokenCountStr]
	if !ok {
		firstLayerNode = createNode()
		rootNode.keyToChildNode[tokenCountStr] = firstLayerNode
	}
	curNode := firstLayerNode

	// handle case of empty log string
	if tokenCount == 0 {
		curNode.clusterIDs = append(curNode.clusterIDs, cluster.id)
		return
	}

	currentDepth := 1
	for _, token := range cluster.logTemplateTokens {
		// if at max depth or this is last token in template - add current log cluster to the leaf node
		if (currentDepth >= d.config.maxNodeDepth) || currentDepth >= tokenCount {
			// clean up stale clusters before adding a new one.
			newClusterIDs := make([]int, 0, len(curNode.clusterIDs))
			for _, clusterID := range curNode.clusterIDs {
				if d.idToCluster.Peek(clusterID) != nil {
					newClusterIDs = append(newClusterIDs, clusterID)
				}
			}
			newClusterIDs = append(newClusterIDs, cluster.id)
			curNode.clusterIDs = newClusterIDs
			break
		}

		// if token not matched in this layer of existing tree.
		if _, ok = curNode.keyToChildNode[token]; !ok {
			// if token not matched in this layer of existing tree.
			if d.config.PreserveNumericTokens || !d.hasNumbers(token) {
				if _, ok = curNode.keyToChildNode[d.config.ParamString]; ok {
					if len(curNode.keyToChildNode) < d.config.MaxChildren {
						newNode := createNode()
						curNode.keyToChildNode[token] = newNode
						curNode = newNode
					} else {
						curNode = curNode.keyToChildNode[d.config.ParamString]
					}
				} else {
					if len(curNode.keyToChildNode)+1 < d.config.MaxChildren {
						newNode := createNode()
						curNode.keyToChildNode[token] = newNode
						curNode = newNode
					} else if len(curNode.keyToChildNode)+1 == d.config.MaxChildren {
						newNode := createNode()
						curNode.keyToChildNode[d.config.ParamString] = newNode
						curNode = newNode
					} else {
						curNode = curNode.keyToChildNode[d.config.ParamString]
					}
				}
			} else {
				if _, ok = curNode.keyToChildNode[d.config.ParamString]; !ok {
					newNode := createNode()
					curNode.keyToChildNode[d.config.ParamString] = newNode
					curNode = newNode
				} else {
					curNode = curNode.keyToChildNode[d.config.ParamString]
				}
			}
		} else {
			// if the token is matched
			curNode = curNode.keyToChildNode[token]
		}

		currentDepth++
	}
}

func (d *Drain) hasNumbers(s string) bool {
	for _, c := range s {
		if unicode.IsNumber(c) {
			return true
		}
	}
	return false
}

func (d *Drain) createTemplate(seq1, seq2 []string) []string {
	if len(seq1) != len(seq2) {
		panic("seq1 seq2 be of same length")
	}
	for i := range seq1 {
		if seq1[i] == seq2[i] || seq2[i] == d.config.ParamString {
			continue
		}
		retVal := make([]string, len(seq2))
		copy(retVal, seq2)
		retVal[i] = d.config.ParamString
		for j := i + 1; j < len(seq1); j++ {
			if seq1[j] != seq2[j] && retVal[j] != d.config.ParamString {
				retVal[j] = d.config.ParamString
			}
		}
		return retVal
	}
	return seq2
}
