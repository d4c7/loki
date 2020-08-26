package stages

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const (
	StageTypeJSON      = "json"
	StageTypeRegex     = "regex"
	StageTypeReplace   = "replace"
	StageTypeMetric    = "metrics"
	StageTypeLabel     = "labels"
	StageTypeTimestamp = "timestamp"
	StageTypeOutput    = "output"
	StageTypeDocker    = "docker"
	StageTypeCRI       = "cri"
	StageTypeMatch     = "match"
	StageTypeTemplate  = "template"
	StageTypePipeline  = "pipeline"
	StageTypeTenant    = "tenant"
)

// Stage takes an existing set of labels, timestamp and log entry and returns either a possibly mutated
// timestamp and log entry
type Stage interface {
	Process(labels model.LabelSet, extracted map[string]interface{}, time *time.Time, entry *string)
	Name() string
}

type StagePlugins interface {
	NewStage(cfg *StageConfig) (Stage, error)
}

// StageFunc is modelled on http.HandlerFunc.
type StageFunc func(labels model.LabelSet, extracted map[string]interface{}, time *time.Time, entry *string)

// Process implements EntryHandler.
func (s StageFunc) Process(labels model.LabelSet, extracted map[string]interface{}, time *time.Time, entry *string) {
	s(labels, extracted, time, entry)
}

type StageCreator func(cfg *StageConfig) (Stage, error)

type PluginDescriptor struct {
	Name    string
	Version string
	Stagger StageCreator
}

// New creates a new stage for the given type and configuration.
func New(cfg *StageConfig) (Stage, error) {
	var s Stage
	var err error
	if cfg.Plugins != nil {
		s, err = cfg.Plugins.NewStage(cfg)

		if err != nil {
			return nil, err
		}
		if s != nil {
			return s, nil
		}
	}
	switch cfg.StageType {
	case StageTypeDocker:
		s, err = NewDocker(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeCRI:
		s, err = NewCRI(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeJSON:
		s, err = newJSONStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeRegex:
		s, err = newRegexStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeMetric:
		s, err = newMetricStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeLabel:
		s, err = newLabelStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeTimestamp:
		s, err = newTimestampStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeOutput:
		s, err = newOutputStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeMatch:
		s, err = newMatcherStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeTemplate:
		s, err = newTemplateStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeTenant:
		s, err = newTenantStage(cfg)
		if err != nil {
			return nil, err
		}
	case StageTypeReplace:
		s, err = newReplaceStage(cfg)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("Unknown stage type: %s", cfg.StageType)
	}
	return s, nil
}
