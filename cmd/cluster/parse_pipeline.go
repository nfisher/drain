package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/faceair/drain/internal/parseio"
)

type parseRunErrors struct {
	mu     sync.Mutex
	err    error
	cancel context.CancelFunc
}

func (e *parseRunErrors) set(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return
	}
	e.err = err
	if e.cancel != nil {
		e.cancel()
	}
}

func (e *parseRunErrors) get() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

type synchronizedParseSink struct {
	mu   *sync.Mutex
	sink parseSink
}

func (s *synchronizedParseSink) Write(ctx context.Context, output parseOutput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Write(ctx, output)
}

func (s *synchronizedParseSink) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Close(ctx)
}

type fanoutParseSink struct {
	sinks []parseSink
}

func (s fanoutParseSink) Write(ctx context.Context, output parseOutput) error {
	for _, sink := range s.sinks {
		if err := sink.Write(ctx, output); err != nil {
			return err
		}
	}
	return nil
}

func (s fanoutParseSink) Close(context.Context) error {
	return nil
}

func runParsePipelines(ctx context.Context, stdout, stderr io.Writer, pipelines []parsePipelineOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := &parseRunErrors{cancel: cancel}
	var stdoutMu sync.Mutex
	var stderrMu sync.Mutex
	var sinkMu sync.Mutex
	lockedStdout := lockedWriter{mu: &stdoutMu, w: stdout}
	lockedStderr := lockedWriter{mu: &stderrMu, w: stderr}

	var wg sync.WaitGroup
	for _, pipeline := range pipelines {
		pipeline := pipeline
		wg.Add(1)
		go func() {
			defer wg.Done()
			runParsePipeline(ctx, pipeline, lockedStdout, lockedStderr, &sinkMu, errs)
		}()
	}
	wg.Wait()
	return errs.get()
}

func runParsePipeline(ctx context.Context, pipeline parsePipelineOptions, stdout, stderr io.Writer, sinkMu *sync.Mutex, errs *parseRunErrors) {
	model, compiledRules, err := readModel(pipeline.ModelPath)
	if err != nil {
		errs.set(parsePipelineError(pipeline.Name, err))
		return
	}

	sinks := make([]parseSink, 0, len(pipeline.Sinks))
	for i, sinkOpts := range pipeline.Sinks {
		if sinkOpts.Now == nil {
			sinkOpts.Now = time.Now
		}
		sink, err := newParseSink(ctx, stdout, sinkOpts)
		if err != nil {
			errs.set(parsePipelineError(pipeline.Name, fmt.Errorf("sink[%d] %q: %w", i, sinkOpts.Format, err)))
			closeParseSinks(sinks, errs, pipeline.Name)
			return
		}
		sinks = append(sinks, &synchronizedParseSink{mu: sinkMu, sink: sink})
	}

	fanout := fanoutParseSink{sinks: sinks}
	var wg sync.WaitGroup
	for _, sourceOpts := range pipeline.Sources {
		sourceOpts := sourceOpts
		wg.Add(1)
		go func() {
			defer wg.Done()
			runParsePipelineSource(ctx, pipeline.Name, sourceOpts, model, compiledRules, fanout, stderr, errs)
		}()
	}
	wg.Wait()
	closeParseSinks(sinks, errs, pipeline.Name)
}

func runParsePipelineSource(ctx context.Context, pipelineName string, sourceOpts parseSourceOptions, model modelFile, compiledRules []compiledMaskingRule, sink parseSink, stderr io.Writer, errs *parseRunErrors) {
	source, err := newParseSource(sourceOpts)
	if err != nil {
		errs.set(parsePipelineError(pipelineName, err))
		return
	}
	sourceInfo := source.Info()

	processor, err := newParseProcessor(model, compiledRules)
	if err != nil {
		_ = source.Close(context.Background())
		errs.set(parsePipelineSourceError(pipelineName, sourceInfo, err))
		return
	}

	parsedLines := 0
	var parsedBytes int64
	var record parseio.SourceRecord
	var output parseOutput
	started := time.Now()
	processErr := parseSourceRecords(ctx, source, processor, sink, &record, &output, func(record parseio.SourceRecord) {
		parsedLines++
		parsedBytes += record.Bytes
	})
	sourceCloseErr := source.Close(context.Background())
	if processErr != nil {
		errs.set(parsePipelineSourceError(pipelineName, sourceInfo, processErr))
		return
	}
	if sourceCloseErr != nil {
		errs.set(parsePipelineSourceError(pipelineName, sourceInfo, sourceCloseErr))
		return
	}
	traceParseSpeedWithPipeline(stderr, pipelineName, sourceInfo, parsedLines, sourceTraceBytes(sourceInfo, parsedBytes), time.Since(started))
}

func closeParseSinks(sinks []parseSink, errs *parseRunErrors, pipelineName string) {
	for i, sink := range sinks {
		if err := sink.Close(context.Background()); err != nil {
			errs.set(parsePipelineError(pipelineName, fmt.Errorf("close sink[%d]: %w", i, err)))
		}
	}
}

func parsePipelineError(pipelineName string, err error) error {
	if pipelineName == "" {
		return err
	}
	return fmt.Errorf("pipeline %q: %w", pipelineName, err)
}

func parsePipelineSourceError(pipelineName string, sourceInfo parseio.SourceInfo, err error) error {
	sourceName := sourceInfo.Name
	if sourceName == "" {
		sourceName = sourceInfo.Kind
	}
	if sourceName == "" {
		return parsePipelineError(pipelineName, err)
	}
	return parsePipelineError(pipelineName, fmt.Errorf("source %q: %w", sourceName, err))
}
