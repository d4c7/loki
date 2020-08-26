package plugins

//go:generate go run resolver/plugins_resolver.go

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/pkg/logentry/stages"
	"github.com/pkg/errors"
)

type PluginManager struct {
	stages map[string]stages.PluginDescriptor
	logger log.Logger
}

func (m PluginManager) NewStage(cfg *stages.StageConfig) (stages.Stage, error) {
	v, ok := m.stages[cfg.StageType]
	if !ok {
		return nil, nil
	}
	if v.Stagger == nil {
		return nil, nil
	}
	stage, err := v.Stagger(cfg)
	if err != nil {
		return nil, err
	}
	return stage, nil
}

func NewPluginManager(logger log.Logger) (*PluginManager, error) {
	i := &PluginManager{
		stages: map[string]stages.PluginDescriptor{},
		logger: log.With(logger, "component", "plugin_manager"),
	}
	for _, descriptor := range resolvedPlugins {
		if err := i.Register(descriptor); err != nil {
			return nil, err
		}
	}
	return i, nil
}

func (m *PluginManager) Register(descriptor stages.PluginDescriptor) error {
	_, ok := m.stages[descriptor.Name]
	if ok {
		level.Warn(m.logger).Log("msg", "already defined plugin "+descriptor.Name+" replaced")
		return errors.New("already defined plugin")
	}
	m.stages[descriptor.Name] = descriptor
	return nil
}
