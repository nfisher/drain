package drain

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/hashicorp/golang-lru/simplelru"
)

type Config struct {
	maxNodeDepth    int
	LogClusterDepth int
	SimTh           float64
	MaxChildren     int
	ExtraDelimiters []string
	MaxClusters     int
	ParamString     string
	// MaskingRules replace known variable patterns before tokenization.
	MaskingRules []MaskingRule
}

// MaskingRule describes a regex replacement applied before Drain tokenization.
type MaskingRule struct {
	Pattern string
	// MaskWith is inserted literally. When empty, Config.ParamString is used.
	MaskWith string
}

type LogCluster struct {
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
	return strings.Join(c.logTemplateTokens, " ")
}
func (c *LogCluster) String() string {
	return fmt.Sprintf("id={%d} : size={%d} : %s", c.id, c.size, c.getTemplate())
}

// Snapshot returns a copy of the cluster state.
func (c *LogCluster) Snapshot() LogClusterSnapshot {
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
	cache, _ := simplelru.NewLRU(maxSize, nil)
	return &LogClusterCache{
		cache: cache,
	}
}

type LogClusterCache struct {
	cache simplelru.LRUCache
}

func (c *LogClusterCache) Values() []*LogCluster {
	values := make([]*LogCluster, 0)
	for _, key := range c.cache.Keys() {
		if value, ok := c.cache.Peek(key); ok {
			values = append(values, value.(*LogCluster))
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
	return cluster.(*LogCluster)
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
	config.maxNodeDepth = config.LogClusterDepth - 2

	d := &Drain{
		config:       config,
		rootNode:     createNode(),
		idToCluster:  createLogClusterCache(config.MaxClusters),
		maskingRules: compileMaskingRules(config.MaskingRules, config.ParamString),
	}
	return d
}

type Drain struct {
	config          *Config
	rootNode        *Node
	idToCluster     *LogClusterCache
	clustersCounter int
	maskingRules    []compiledMaskingRule
}

type compiledMaskingRule struct {
	regex       *regexp.Regexp
	replacement string
}

func (d *Drain) Clusters() []*LogCluster {
	return d.idToCluster.Values()
}

// ClusterSnapshots returns a stable, ID-sorted copy of all cluster states.
func (d *Drain) ClusterSnapshots() []LogClusterSnapshot {
	clusters := d.Clusters()
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
		newTemplateTokens := d.createTemplate(contentTokens, matchCluster.logTemplateTokens)
		matchCluster.logTemplateTokens = newTemplateTokens
		matchCluster.size++
		// Touch cluster to update its state in the cache.
		d.idToCluster.Get(matchCluster.id)
	}
	return matchCluster
}

// Match against an already existing cluster. Match shall be perfect (sim_th=1.0). New cluster will not be created as a result of this call, nor any cluster modifications.
func (d *Drain) Match(content string) *LogCluster {
	contentTokens := d.getContentAsTokens(content)
	matchCluster := d.treeSearch(d.rootNode, contentTokens, 1.0, true)
	return matchCluster
}

func (d *Drain) getContentAsTokens(content string) []string {
	content = strings.TrimSpace(content)
	for _, maskingRule := range d.maskingRules {
		content = maskingRule.regex.ReplaceAllStringFunc(content, func(string) string {
			return maskingRule.replacement
		})
	}
	for _, extraDelimiter := range d.config.ExtraDelimiters {
		content = strings.Replace(content, extraDelimiter, " ", -1)
	}
	return strings.Split(content, " ")
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
		replacement := rule.MaskWith
		if replacement == "" {
			replacement = defaultReplacement
		}
		compiled = append(compiled, compiledMaskingRule{
			regex:       regex,
			replacement: replacement,
		})
	}
	return compiled
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
		return d.idToCluster.Get(curNode.clusterIDs[0])
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

// fastMatch Find the best match for a log message (represented as tokens) versus a list of clusters
func (d *Drain) fastMatch(clusterIDs []int, tokens []string, simTh float64, includeParams bool) *LogCluster {
	var matchCluster, maxCluster *LogCluster

	maxSim := -1.0
	maxParamCount := -1
	for _, clusterID := range clusterIDs {
		// Try to retrieve cluster from cache with bypassing eviction
		// algorithm as we are only testing candidates for a match.
		cluster := d.idToCluster.Get(clusterID)
		if cluster == nil {
			continue
		}
		curSim, paramCount := d.getSeqDistance(cluster.logTemplateTokens, tokens, includeParams)
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
				if d.idToCluster.Get(clusterID) != nil {
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
			if !d.hasNumbers(token) {
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
	retVal := make([]string, len(seq2))
	copy(retVal, seq2)
	for i := range seq1 {
		if seq1[i] != seq2[i] {
			retVal[i] = d.config.ParamString
		}
	}
	return retVal
}
