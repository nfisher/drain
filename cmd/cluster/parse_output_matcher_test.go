package main

import (
	"fmt"

	"github.com/faceair/drain"
	a "github.com/gogunit/gunit/hammy"
)

func ParseOutput(actual parseOutput) *parseOutputMatchy {
	return &parseOutputMatchy{actual: actual}
}

type parseOutputMatchy struct {
	actual parseOutput
}

func (m *parseOutputMatchy) HasTemplateID(expected int) a.AssertionMessage {
	if m.actual.TemplateID == nil {
		return a.Assert(false, "got nil template ID, wanted <%d> for %s", expected, describeParseOutput(m.actual))
	}
	return a.Assert(*m.actual.TemplateID == expected, "got template ID <%d>, wanted <%d> for %s", *m.actual.TemplateID, expected, describeParseOutput(m.actual))
}

func (m *parseOutputMatchy) HasVariables(expected ...string) a.AssertionMessage {
	return a.Slice(m.actual.Variables).EqualTo(expected...)
}

func (m *parseOutputMatchy) HasParameters(expected ...drain.ExtractedParameter) a.AssertionMessage {
	return a.Slice(m.actual.Parameters).EqualTo(expected...)
}

func (m *parseOutputMatchy) IsUnmatched() a.AssertionMessage {
	return a.Assert(m.actual.TemplateID == nil && m.actual.Parameters == nil && m.actual.Variables != nil && len(m.actual.Variables) == 0,
		"got %s, wanted unmatched parse output with nil template ID, nil parameters, and empty non-nil variables", describeParseOutput(m.actual))
}

func describeParseOutput(output parseOutput) string {
	templateID := "<nil>"
	if output.TemplateID != nil {
		templateID = fmt.Sprintf("%d", *output.TemplateID)
	}
	return fmt.Sprintf("parseOutput{templateID:%s modelID:%q variables:%#v parameters:%#v}", templateID, output.ModelID, output.Variables, output.Parameters)
}
