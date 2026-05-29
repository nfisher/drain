package main

import (
	"fmt"

	"github.com/faceair/drain"
)

type parseProcessor struct {
	model          modelFile
	compiledRules  []compiledMaskingRule
	logger         *drain.Drain
	parseTemplates map[int]parseTemplate
}

type variableExtractionError struct {
	clusterID int
	template  string
}

func (e variableExtractionError) Error() string {
	return fmt.Sprintf("matched cluster %d did not match during variable extraction: template=%q", e.clusterID, e.template)
}

func newParseProcessor(model modelFile, compiledRules []compiledMaskingRule) (*parseProcessor, error) {
	logger, err := drainFromModel(model)
	if err != nil {
		return nil, err
	}
	parseTemplates, _ := parseTemplatesFromModel(model)
	return &parseProcessor{
		model:          model,
		compiledRules:  compiledRules,
		logger:         logger,
		parseTemplates: parseTemplates,
	}, nil
}

func (p *parseProcessor) Parse(line string, out *parseOutput) error {
	variables := out.Variables[:0]
	if variables == nil {
		variables = []string{}
	}
	*out = parseOutput{
		ModelID:   p.model.ModelID,
		Variables: variables,
	}

	cluster := p.logger.MatchWithOptions(line, drain.MatchOptions{
		FullSearchStrategy: drain.FullSearchFallback,
	})
	if cluster == nil {
		return nil
	}

	clusterID := cluster.ID()
	parseTemplate, ok := p.parseTemplates[clusterID]
	if !ok {
		return fmt.Errorf("matched cluster %d was not found in model", clusterID)
	}

	parameters, ok := p.logger.ExtractParameters(parseTemplate.template, line)
	if ok {
		out.Variables = appendParameterValues(out.Variables, parameters)
		if hasNamedParameters(parameters) {
			out.Parameters = parameters
		}
	} else {
		var variables []string
		variables, ok = matchTemplate(p.model.ParamString, parseTemplate.tokens, tokenizeLine(line, p.compiledRules, p.model.ExtraDelimiters), out.Variables)
		if !ok {
			return variableExtractionError{
				clusterID: clusterID,
				template:  parseTemplate.template,
			}
		}
		out.Variables = variables
	}

	templateID := parseTemplate.id
	out.TemplateID = &templateID
	return nil
}
