package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/faceair/drain"
)

type templateDistribution struct {
	TemplateID int    `json:"template_id"`
	ModelID    string `json:"model_id"`
	Template   string `json:"template"`
	Count      int    `json:"count"`
}

type testOutput struct {
	Total     int                    `json:"total"`
	Matched   int                    `json:"matched"`
	Unmatched int                    `json:"unmatched"`
	Templates []templateDistribution `json:"templates"`
}

func runTrain(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("train", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filename := fs.String("filename", "example.log", "training log file")
	modelPath := fs.String("model", "model.json", "model output path")
	update := fs.Bool("update", false, "load and update the existing model")
	metadataPath := fs.String("metadata", "", "metadata JSON object to merge into the model")
	maskingRulesPath := fs.String("masking-rules", "", "masking rules JSON array to use instead of defaults")
	defaultConfig := clusterConfig()
	simTh := fs.Float64("sim-th", defaultConfig.SimTh, "training similarity threshold")
	depth := fs.Int("depth", defaultConfig.LogClusterDepth, "max depth levels of log clusters")
	maxChildren := fs.Int("max-children", defaultConfig.MaxChildren, "max number of children of an internal node")
	parametrizeNumericTokens := fs.Bool("parametrize-numeric-tokens", !defaultConfig.PreserveNumericTokens, "treat tokens containing digits as template parameters")
	var extraDelimiters extraDelimiterFlags
	fs.Var(&extraDelimiters, "extra-delimiter", "literal delimiter to split on after masking; repeat for multiple delimiters")
	if err := fs.Parse(args); err != nil {
		return err
	}
	simThProvided := flagWasProvided(fs, "sim-th")
	depthProvided := flagWasProvided(fs, "depth")
	maxChildrenProvided := flagWasProvided(fs, "max-children")
	parametrizeNumericTokensProvided := flagWasProvided(fs, "parametrize-numeric-tokens")
	extraDelimitersProvided := flagWasProvided(fs, "extra-delimiter")
	maskingRulesProvided := flagWasProvided(fs, "masking-rules")
	if err := validateSimTh("sim-th", *simTh); err != nil {
		return err
	}
	if err := validateDepth("depth", *depth); err != nil {
		return err
	}
	if err := validateMaxChildren("max-children", *maxChildren); err != nil {
		return err
	}
	if err := validateExtraDelimiters("extra-delimiter", extraDelimiters); err != nil {
		return err
	}
	var maskingRulesFromFile []modelMaskingRule
	if maskingRulesProvided {
		var err error
		maskingRulesFromFile, err = readMaskingRulesFile(*maskingRulesPath)
		if err != nil {
			return err
		}
	}

	config := defaultConfig
	config.SimTh = *simTh
	config.LogClusterDepth = *depth
	config.MaxChildren = *maxChildren
	config.PreserveNumericTokens = !*parametrizeNumericTokens
	if extraDelimitersProvided {
		config.ExtraDelimiters = copyStrings(extraDelimiters)
	}
	if maskingRulesProvided {
		config.MaskingRules = drainMaskingRules(maskingRulesFromFile)
	}
	logger := drain.New(config)
	var metadata map[string]json.RawMessage
	if *update {
		existingModel, _, err := readModel(*modelPath)
		if err != nil {
			return err
		}
		metadata = copyMetadata(existingModel.Metadata)
		config = configFromModel(existingModel)
		if simThProvided {
			config.SimTh = *simTh
		}
		if depthProvided {
			config.LogClusterDepth = *depth
		}
		if maxChildrenProvided {
			config.MaxChildren = *maxChildren
		}
		if parametrizeNumericTokensProvided {
			config.PreserveNumericTokens = !*parametrizeNumericTokens
		}
		if extraDelimitersProvided {
			config.ExtraDelimiters = copyStrings(extraDelimiters)
		}
		if maskingRulesProvided {
			config.MaskingRules = drainMaskingRules(maskingRulesFromFile)
		}
		logger = drain.New(config)
		if err := logger.LoadClusters(snapshotsFromModel(existingModel)); err != nil {
			return err
		}
	}

	var metadataFromFile map[string]json.RawMessage
	if *metadataPath != "" {
		var err error
		metadataFromFile, err = readMetadataFile(*metadataPath)
		if err != nil {
			return err
		}
	}

	if err := scanLines(*filename, func(line string) error {
		logger.Train(line)
		return nil
	}); err != nil {
		return err
	}

	model := modelFromDrain(config, logger)
	model.Metadata = metadataWithTimestamps(metadata, metadataFromFile, *update, time.Now().UTC())
	if err := writeModel(*modelPath, model); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %d templates to %s\n", len(model.Templates), *modelPath)
	return nil
}

func flagWasProvided(fs *flag.FlagSet, name string) bool {
	provided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}

func runTest(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filename := fs.String("filename", "example.log", "target log file")
	modelPath := fs.String("model", "model.json", "model path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	model, _, err := readModel(*modelPath)
	if err != nil {
		return err
	}
	logger, err := drainFromModel(model)
	if err != nil {
		return err
	}

	counts := make(map[int]int, len(model.Templates))
	for _, template := range model.Templates {
		counts[template.ID] = 0
	}

	output := testOutput{
		Templates: make([]templateDistribution, 0, len(model.Templates)),
	}
	if err := scanLines(*filename, func(line string) error {
		output.Total++
		cluster := logger.MatchWithOptions(line, drain.MatchOptions{
			FullSearchStrategy: drain.FullSearchFallback,
		})
		if cluster == nil {
			output.Unmatched++
			return nil
		}
		output.Matched++
		counts[cluster.ID()]++
		return nil
	}); err != nil {
		return err
	}

	for _, template := range model.Templates {
		output.Templates = append(output.Templates, templateDistribution{
			TemplateID: template.ID,
			ModelID:    model.ModelID,
			Template:   template.Template,
			Count:      counts[template.ID],
		})
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}
