package drain

import (
	"fmt"

	a "github.com/gogunit/gunit/hammy"
)

func Cluster(actual *LogCluster) *logClusterMatchy {
	return &logClusterMatchy{actual: actual}
}

type logClusterMatchy struct {
	actual *LogCluster
}

func (m *logClusterMatchy) Exists() a.AssertionMessage {
	return a.Assert(m.actual != nil, "got nil cluster, wanted a log cluster")
}

func (m *logClusterMatchy) HasID(expected int) a.AssertionMessage {
	if m.actual == nil {
		return a.Assert(false, "got nil cluster, wanted id <%d>", expected)
	}
	return a.Assert(m.actual.id == expected, "got cluster id <%d>, wanted <%d> for %s", m.actual.id, expected, describeLogCluster(m.actual))
}

func (m *logClusterMatchy) HasSize(expected int) a.AssertionMessage {
	if m.actual == nil {
		return a.Assert(false, "got nil cluster, wanted size <%d>", expected)
	}
	return a.Assert(m.actual.size == expected, "got cluster size <%d>, wanted <%d> for %s", m.actual.size, expected, describeLogCluster(m.actual))
}

func (m *logClusterMatchy) HasTemplate(expected string) a.AssertionMessage {
	if m.actual == nil {
		return a.Assert(false, "got nil cluster, wanted template <%s>", expected)
	}
	actual := m.actual.Template()
	return a.Assert(actual == expected, "got cluster template <%s>, wanted <%s> for %s", actual, expected, describeLogCluster(m.actual))
}

func (m *logClusterMatchy) IsSamePointerAs(expected *LogCluster) a.AssertionMessage {
	return a.Assert(m.actual == expected, "got cluster pointer <%p>, wanted same pointer as <%p>", m.actual, expected)
}

func (m *logClusterMatchy) IsNotSamePointerAs(expected *LogCluster) a.AssertionMessage {
	return a.Assert(m.actual != expected, "got cluster pointer <%p>, wanted different pointer than <%p>", m.actual, expected)
}

func describeLogCluster(cluster *LogCluster) string {
	if cluster == nil {
		return "<nil>"
	}
	return fmt.Sprintf("cluster{id:%d size:%d template:%q}", cluster.id, cluster.size, cluster.Template())
}
