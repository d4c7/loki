package stages

/*
WARNING: Script Stage is Experimental, requires careful profiling
*/
import (
	"fmt"
	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/go-kit/kit/log/level"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const (
	ErrScriptExecFailed = "script exec failed"
)

type ScriptConfig struct {
	Text  string                 `mapstructure:"text"`
	State map[string]interface{} `mapstructure:"state"`
	Debug bool                   `mapstructure:"debug"`
}

type scriptStage struct {
	logger log.Logger
	script *tengo.Compiled
}

func newScriptStage(logger log.Logger, config interface{}) (Stage, error) {
	cfg, err := parseScriptConfig(config)
	if err != nil {
		return nil, err
	}

	if cfg.Text == "" {
		return nil, errors.New("script text required")
	}

	src := cfg.Text

	script := tengo.NewScript([]byte(src))

	//TODO:WARN:potential security problem here ojo!
	script.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))

	//main variables
	script.Add("label", map[string]interface{}{})
	script.Add("source", map[string]interface{}{})
	script.Add("entry", "")
	script.Add("timestamp", time.Now())

	//initial state variables
	for k, v := range cfg.State {
		script.Add(k, v)
	}

	//compile script
	compiled, err := script.Compile()
	if err != nil {
		return nil, err
	}

	//setup script stage
	ss := &scriptStage{
		script: compiled,
		logger: log.With(logger, "component", "stage", "type", "script"),
	}

	//test script for empty params
	err = ss.runScript(model.LabelSet{"__PROMTAIL_SCRIPT_TEST__": "true"}, map[string]interface{}{}, nil, nil)
	if err != nil {
		return nil, err
	}

	//reset script state
	for k, v := range cfg.State {
		compiled.Set(k, v)
	}

	return ss, nil
}

func parseScriptConfig(config interface{}) (*ScriptConfig, error) {
	cfg := &ScriptConfig{}
	err := mapstructure.Decode(config, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (r *scriptStage) runScript(labels model.LabelSet, extracted map[string]interface{}, t *time.Time, entry *string) error {
	//set log values
	lm := map[string]interface{}{}
	for k, v := range labels {
		lm[string(k)] = string(v)
	}

	script := r.script

	err := script.Set("label", lm)
	if err != nil {
		return err
	}

	src := map[string]interface{}{}
	for k, v := range extracted {
		src[k], err = fromInterface(v)
		if err != nil {
			if Debug {
				level.Debug(r.logger).Log(fmt.Sprintf("cannot convert %v value %v", k, v))
			}
		}
	}

	err = script.Set("source", extracted)
	if err != nil {
		return err
	}
	var ctime time.Time
	if t == nil {
		ctime = time.Now()
	} else {
		ctime = *t
	}
	err = script.Set("timestamp", ctime)
	if err != nil {
		return err
	}
	sentry := ""
	if entry != nil {
		sentry = *entry
	}
	err = script.Set("entry", sentry)
	if err != nil {
		return err
	}

	// run the script
	err = script.Run()
	if err != nil {
		return err
	}

	// retrieve values
	if entry != nil {
		*entry = script.Get("entry").String()
	}
	if t != nil {
		v := script.Get("timestamp").Object()
		if v.TypeName() == "time" {
			t2, ok := v.(*tengo.Time)
			if !ok {
				return errors.New("invalid timestamp")
			}
			*t = t2.Value
		} else {
			return errors.New("invalid timestamp")
		}
	}
	if labels != nil {
		rlabels := script.Get("label").Map()
		for k, v := range rlabels {
			ln := model.LabelName(k)
			if !ln.IsValid() {
				return errors.New("invalid label name " + k)
			}
			vs, err := getString(v)
			if err != nil {
				return err
			}
			lv := model.LabelValue(vs)
			if !lv.IsValid() {
				return errors.New("invalid label value " + vs)
			}
			labels[ln] = lv
		}
	}

	if extracted != nil {
		rsource := script.Get("source").Map()
		for k, v := range rsource {
			extracted[k] = v
		}
	}
	return nil
}

// Process implements Stage
func (r *scriptStage) Process(labels model.LabelSet, extracted map[string]interface{}, t *time.Time, entry *string) {
	err := r.runScript(labels, extracted, t, entry)
	if err != nil {
		if Debug {
			level.Debug(r.logger).Log("msg", ErrScriptExecFailed, "err", err)
		}
	}

}

// Name implements Stage
func (r *scriptStage) Name() string {
	return StageTypeScript
}

//workaround for tengo from interface unsupported arrays
func fromInterface(v interface{}) (tengo.Object, error) {
	switch v := v.(type) {
	case []string:
		arr := make([]tengo.Object, len(v))
		for i, e := range v {
			vo, err := fromInterface(e)
			if err != nil {
				return nil, err
			}
			arr[i] = vo
		}
		return &tengo.Array{Value: arr}, nil

		//...
	}
	return tengo.FromInterface(v)
}
