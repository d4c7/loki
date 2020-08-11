package multiline

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"regexp"
	"sync"
	"time"
)

const (
	ErrEmptyMultiLineParserStageConfig              = "empty json stage configuration"
	ErrCouldNotCompileMultiLineParserGroupRegex     = "could not compile coalesce group_expression"
	ErrCouldNotCompileMultiLineParserContRegex      = "could not compile coalesce continue_expression"
	ErrCouldNotCompileMultiLineParserSubFilterRegex = "could not compile coalesce subline_filter_expression"
	ErrMultiLineParserStageUnconfig                 = "need any group_expression or continue_expression"
)

//TODO: note //!!--positions.sync-period", 10*time.Second
type Config struct {
	GroupKeyExpression     string `yaml:"group_expression"`
	ContinueLineExpression string `yaml:"continue_expression"`
	AppendExpression       string `yaml:"append_expression"`
	PreserveContinue       bool   `yaml:"preserve_continue"`
	FlushPeriod            int    `yaml:"flush_period"`
	JoinText               string `yaml:"join_text"`
	MultiTrack             bool   `yaml:"multitrack"`
}

type multiLine struct {
	mode              int
	cfg               *Config
	logger            log.Logger
	groupKeyRegex     *regexp.Regexp
	continueLineRegex *regexp.Regexp
	appendRegex       *regexp.Regexp
	trackers          map[string]*track
	lastGroupKey      string
	quit              chan bool
	list              []*track
	flush             time.Duration
	next              api.EntryHandler
	sync.Mutex
}

type track struct {
	inTime    time.Time
	labels    model.LabelSet
	timestamp time.Time
	entry     string
}

func (c multiLine) launchCleaner() {
	cleaner := time.NewTicker(time.Duration(c.cfg.FlushPeriod*1000/2) * time.Millisecond)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-cleaner.C:
				c.clean(false)
			case <-quit:
				cleaner.Stop()
				return
			}
		}
	}()
	c.quit = quit
}

func (c *multiLine) Flush() {
	c.clean(true)
}

func (c *multiLine) clean(b bool) {
	level.Warn(c.logger).Log("msg", "run cleaner")

	c.Lock()
	now := time.Now()
	for k, t := range c.trackers {
		if b || t.inTime.After(now) {
			delete(c.trackers, k)
			c.list = append(c.list, t)
		}
	}
	c.Unlock()
}

func NewMultiLineParser(logger log.Logger, config *Config, next api.EntryHandler) (api.EntryHandler, error) {
	if config == nil {
		return nil, errors.New(ErrEmptyMultiLineParserStageConfig)
	}

	ml := &multiLine{
		cfg:      config,
		logger:   log.With(logger, "component", "multiline"),
		list:     make([]*track, 0),
		trackers: map[string]*track{},
		next:     next,
	}

	if len(config.GroupKeyExpression) > 0 {
		expr, err := regexp.Compile(config.GroupKeyExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineParserGroupRegex)
		}
		ml.groupKeyRegex = expr
	}

	if len(config.ContinueLineExpression) > 0 {
		expr, err := regexp.Compile(config.ContinueLineExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineParserContRegex)
		}
		ml.continueLineRegex = expr
	}

	if len(config.AppendExpression) > 0 {
		expr, err := regexp.Compile(config.AppendExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineParserSubFilterRegex)
		}
		ml.appendRegex = expr
	}

	if "" == config.GroupKeyExpression && "" == config.ContinueLineExpression {
		return nil, errors.New(ErrMultiLineParserStageUnconfig)
	}

	if config.FlushPeriod == 0 {
		config.FlushPeriod = 30
	}

	if config.FlushPeriod > 0 {
		ml.launchCleaner()
	} else {
		level.Warn(ml.logger).Log("msg", "multiline cleaner disabled")
	}

	return ml, nil
}

func (c *multiLine) Handle(labels model.LabelSet, t time.Time, entry string) error {
	now := time.Now()
	var readyEntries []*track
	workLine := entry

	groupContinue := true
	if c.continueLineRegex != nil {
		_, mat, inv := splitMatch(c.continueLineRegex, workLine)
		groupContinue = mat != ""
		if groupContinue && !c.cfg.PreserveContinue {
			workLine = inv
		}
	}

	groupKey := ""
	workLineInv := entry
	//	groupMatch:=""
	if c.groupKeyRegex != nil {
		var mat string
		groupKey, mat, workLineInv = splitMatch(c.groupKeyRegex, workLine)
		if groupKey == "" && mat != "" {
			c.clean(true)
		}

		if !c.cfg.MultiTrack && c.lastGroupKey != "" && groupKey != c.lastGroupKey {
			c.clean(true)
		}

		c.lastGroupKey = groupKey
	}
	/*
		if groupKey == "" && (c.continueLineRegex == nil) {
			groupContinue = true
		}*/

	//groupContinue := c.continueLineRegex == nil || c.continueLineRegex.Match([]byte(*entry))

	c.Lock()
	readyEntries = c.list[:]
	c.list = c.list[:0]
	prevEntry, _ := c.trackers[groupKey]
	if groupContinue {
		if prevEntry == nil {
			prevEntry = &track{
				inTime:    now,
				timestamp: t,
				labels:    labels,
				entry:     workLine,
			}
			c.trackers[groupKey] = prevEntry
		} else {
			prevEntry.labels.Merge(labels)
			prevEntry.entry = c.joinLine(prevEntry.entry, workLineInv, workLine)
		}
	} else {
		if prevEntry != nil {
			delete(c.trackers, groupKey)
			prevEntry.entry = c.joinLine(prevEntry.entry, workLineInv, workLine)
			readyEntries = append(readyEntries, prevEntry)
		} else {
			readyEntries = append(readyEntries, &track{
				inTime:    now,
				timestamp: t,
				labels:    labels,
				entry:     workLine,
			})
		}
	}
	c.Unlock()

	var err util.MultiError
	if c.next != nil {
		for _, i := range readyEntries {
			err.Add(c.next.Handle(i.labels, i.timestamp, i.entry))
		}
	}
	return err.Err()
}

func joinStr(a string, sep string, b string) string {
	r := a
	if len(a) > 0 {
		r += sep
	}
	r += b
	return r
}

func (c *multiLine) joinLine(a string, b string, full string) string {
	if c.appendRegex != nil {
		sel, mat, _ := splitMatch(c.appendRegex, full)
		if sel == "" {
			sel = mat
		}
		b = sel
	}
	return joinStr(a, c.cfg.JoinText, b)
}

func splitMatch(expression *regexp.Regexp, s string) (string, string, string) {
	match := expression.FindAllSubmatchIndex([]byte(s), -1)
	if match == nil {
		return "", "", s
	}
	sel := ""
	inv := ""
	mat := ""
	left := 0
	for _, i := range match {
		inv = inv + s[left:i[0]]
		if len(i) == 4 {
			sel = sel + s[i[2]:i[3]]
		}
		mat = mat + s[i[0]:i[1]]
		left = i[1]
	}
	inv = inv + s[left:]
	return sel, mat, inv
}
