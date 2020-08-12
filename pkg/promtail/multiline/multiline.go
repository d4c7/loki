package multiline

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	ErrEmptyMultiLineConfig                    = "empty configuration"
	ErrCouldNotCompileMultiLineExpressionRegex = "could not compile expression"
	ErrCouldNotCompileMultiLineFirstLineRegex  = "could not compile first_expression"
	ErrCouldNotCompileMultiLineNextLineRegex   = "could not compile next_expression"
	ErrCouldMultiLineExpressionRequiredRegex   = "expression is required"
	ErrMultiLineUnsupportedMode                = "unsupported mode"
	ErrMultiLineUnvalidMaxWaitTime             = "invalid max_wait_time duration"
)

type FlushableEntryHandler interface {
	Flush() error
	api.EntryHandler
}

//note log lines could be lost if MaxWait > positions.sync-period ( def 10*time.Second)
type Config struct {
	Expression          string `yaml:"expression"`
	FirstLineExpression string `yaml:"first"`
	NextLineExpression  string `yaml:"next"`
	MaxWait             string `yaml:"max_wait"`
	Delimiter           string `yaml:"delimiter"`
	Mode                string `yaml:"mode"`
}

type multiLineParser struct {
	cfg             *Config
	logger          log.Logger
	expressionRegex *regexp.Regexp
	firstLineRegex  *regexp.Regexp
	nextLineRegex   *regexp.Regexp
	multitrack      bool
	multilines      []*multilineEntry
	multiline       *multilineEntry
	quit            chan bool
	maxWait         time.Duration
	next            api.EntryHandler
	handler         func(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) error
	sync.Mutex
}

type multilineEntry struct {
	enrollTime time.Time
	labels     model.LabelSet
	timestamp  time.Time
	key        string
	entry      string
	lines      int
}

func (d *multilineEntry) reset() {
	d.labels = model.LabelSet{}
	d.entry = ""
	d.lines = 0
}

func (d *multilineEntry) set(labels model.LabelSet, t time.Time, entry string) {
	if labels != nil {
		d.labels = labels.Clone()
	} else {
		d.labels = model.LabelSet{}
	}
	d.timestamp = t
	d.entry = entry
	d.lines = 1
	d.enrollTime = time.Now()
}

func (d *multilineEntry) append(labels model.LabelSet, entry string, delimiter string) {
	d.labels = labels.Merge(labels)
	d.entry = join(d.entry, delimiter, entry)
	d.lines++
}

func (c *multiLineParser) startFlusher() {
	flusher := time.NewTicker(c.maxWait / 2)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-flusher.C:
				err := c.flush(false)
				if err != nil {
					level.Debug(c.logger).Log("msg", "failed to flush multiline logs", "err", err)
				}
			case <-quit:
				flusher.Stop()
				return
			}
		}
	}()
	c.quit = quit
}

func (c *multiLineParser) Flush() error {
	return c.flush(true)
}

func (c *multiLineParser) flush(force bool) error {
	now := time.Now()
	c.Lock()
	var err util.MultiError

	if c.multitrack {
		nextGen := make([]*multilineEntry, 0, len(c.multilines))
		for _, t := range c.multilines {
			if t.lines == 0 {
				continue
			}
			if force || now.Sub(t.enrollTime) > c.maxWait {
				err.Add(c.next.Handle(t.labels, t.timestamp, t.entry))
			} else {
				nextGen = append(nextGen, t)
			}
		}
		c.multilines = nextGen
	} else {
		t := c.multiline
		if t.lines > 0 && (force || now.Sub(t.enrollTime) > c.maxWait) {
			err.Add(c.next.Handle(t.labels, t.timestamp, t.entry))
			t.reset()
		}
	}
	c.Unlock()
	return err.Err()
}

func NewMultiLineParser(logger log.Logger, config *Config, next api.EntryHandler) (FlushableEntryHandler, error) {
	if config == nil {
		return nil, errors.New(ErrEmptyMultiLineConfig)
	}

	ml := &multiLineParser{
		cfg:    config,
		logger: log.With(logger, "component", "multiline"),
	}

	if len(config.Expression) > 0 {
		expr, err := regexp.Compile(config.Expression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineExpressionRegex)
		}
		ml.expressionRegex = expr
	} else {
		return nil, errors.New(ErrCouldMultiLineExpressionRequiredRegex)
	}

	if len(config.FirstLineExpression) > 0 {
		expr, err := regexp.Compile(config.FirstLineExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineFirstLineRegex)
		}
		ml.firstLineRegex = expr
	}

	if len(config.NextLineExpression) > 0 {
		expr, err := regexp.Compile(config.NextLineExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineNextLineRegex)
		}
		ml.nextLineRegex = expr
	}

	ml.maxWait = 5 * time.Second
	if config.MaxWait != "" {
		var err error
		ml.maxWait, err = time.ParseDuration(config.MaxWait)
		if err != nil {
			return nil, errors.Wrap(err, ErrMultiLineUnvalidMaxWaitTime)
		}
	}
	if ml.maxWait > 0 {
		ml.startFlusher()
	} else {
		level.Warn(ml.logger).Log("msg", "multiline flusher disabled")
	}

	switch config.Mode {
	case "newline":
		ml.handler = handleNewLineMode
	case "group":
		ml.handler = handleGroupMode
	case "unordered_group":
		ml.handler = handleUnorderedGroupMode
		ml.multitrack = true
	case "continue":
		ml.handler = handleContinueMode
	default:
		return nil, errors.New(ErrMultiLineUnsupportedMode)
	}

	if ml.multitrack {
		ml.multilines = []*multilineEntry{}
	} else {
		ml.multiline = newLine("")
	}

	if next == nil {
		level.Warn(ml.logger).Log("msg", "multiline next handler is not defined")
		next = api.EntryHandlerFunc(func(labels model.LabelSet, time time.Time, entry string) error {
			return nil
		})
	}

	ml.next = next

	return ml, nil
}

func handleNewLineMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	d := c.multiline
	if !c.expressionRegex.Match([]byte(entry)) {
		d.append(labels, selection(c.nextLineRegex, entry), c.cfg.Delimiter)
	} else {
		if d.lines > 0 {
			err = c.next.Handle(d.labels, d.timestamp, d.entry)
		}
		d.set(labels, t, selection(c.firstLineRegex, entry))
	}
	return
}

func handleGroupMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	key, inv := disjoint(c.expressionRegex, entry)
	d := c.multiline
	if d.key == key {
		d.labels = labels.Merge(labels)
		line := inv
		if c.nextLineRegex != nil {
			line = selection(c.nextLineRegex, entry)
		}
		d.append(labels, line, c.cfg.Delimiter)
	} else {
		if d.lines > 0 {
			err = c.next.Handle(d.labels, d.timestamp, d.entry)
		}
		d.set(labels, t, selection(c.firstLineRegex, entry))
		d.key = key
	}
	return
}

func handleUnorderedGroupMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	key, inv := disjoint(c.expressionRegex, entry)
	d := c.fetchLine(key)
	if d.lines > 0 {
		d.labels = labels.Merge(labels)
		line := inv
		if c.nextLineRegex != nil {
			line = selection(c.nextLineRegex, entry)
		}
		d.append(labels, line, c.cfg.Delimiter)
	} else {
		d.set(labels, t, selection(c.firstLineRegex, entry))
		d.key = key
	}
	return
}

func handleContinueMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	d := c.multiline
	line := selection(c.expressionRegex, entry)
	if line != "" {
		if d.lines > 0 {
			d.append(labels, selection(c.nextLineRegex, line), c.cfg.Delimiter)
		} else {
			d.set(labels, t, selection(c.firstLineRegex, line))
		}
	} else {
		if d.lines > 0 {
			d.append(labels, selection(c.nextLineRegex, entry), c.cfg.Delimiter)
			err = c.next.Handle(d.labels, d.timestamp, d.entry)
			d.reset()
		} else {
			err = c.next.Handle(labels, t, entry)
		}
	}
	return
}

func (c *multiLineParser) Handle(labels model.LabelSet, t time.Time, entry string) (err error) {
	c.Lock()
	err = c.handler(c, labels, t, entry)
	c.Unlock()
	return
}

func (c *multiLineParser) fetchLine(key string) *multilineEntry {
	for _, t := range c.multilines {
		if t.key == key {
			return t
		}
	}
	d := newLine(key)
	c.multilines = append(c.multilines, d)
	return d
}

func newLine(key string) *multilineEntry {
	return &multilineEntry{labels: model.LabelSet{}, key: key}
}

func join(a string, sep string, b string) string {
	r := a
	if len(a) > 0 {
		r += sep
	}
	r += b
	return r
}

func disjoint(expression *regexp.Regexp, s string) (string, string) {
	matches := expression.FindAllSubmatchIndex([]byte(s), -1)
	sel := make([]string, 0, len(matches))
	inv := make([]string, 0, len(matches)*2)

	beg := 0
	end := 0
	last := 0

	for _, match := range matches {
		for i, n := 2, len(match); i < n; i += 2 {
			beg = match[i]
			if beg < 0 {
				continue
			}
			end = match[i+1]
			if end > beg && beg >= last {
				inv = append(inv, s[last:beg])
				sel = append(sel, s[beg:end])
				last = end
			}
		}
	}

	if last < len(s) {
		inv = append(inv, s[last:])
	}

	return strings.Join(sel, ""), strings.Join(inv, "")
}

func selection(expression *regexp.Regexp, s string) string {
	if expression == nil {
		return s
	}
	sel, _ := disjoint(expression, s)
	return sel
}
