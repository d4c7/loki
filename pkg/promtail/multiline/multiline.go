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
	ErrMultiLineUnsupportedMode                = "unsupported mode"
	ErrMultiLineUnvalidMaxWaitTime             = "invalid max_idle_duration duration"
	ErrMultiLineModeRequireMaxWait             = "mode require max_idle_duration duration > 0 "
)

// multiline EntryHandler is an api.EntryHandler that allows to flush buffered log lines and be stopped
type EntryHandler interface {
	//Flush orders the immediate drain of the log entries retained
	Flush() error

	//Stop the service
	Stop() error

	api.EntryHandler
}

// Note log lines could be lost if IdleDuration > positions.sync-period ( def 10*time.Second)
type Config struct {
	// Mode determines the main behaviour of the parser. Possible values are:
	// * newline: a new multiline entry starts when a line match a expression
	// * continue: a multiline entry continue with the next log line if the expression match
	// * group: multiline entries are grouped by extracting a group key of each line
	// * unordered_group: like group mode but supporting mixed lines with different group keys
	Mode string `yaml:"mode"`

	// Expression is the main regular expression used for the selected mode of parsing
	Expression string `yaml:"expression"`

	// FirstLineExpression is a regular expression for capturing groups to form the first log line of the multiline log
	FirstLineExpression string `yaml:"first"`

	// NextLineExpression is argular expression for capturing groups to form the second and more log lines of the
	// multiline log
	NextLineExpression string `yaml:"next"`

	// Max duration a multiline log line is hold before sending it to the next handler. Note The parser cannot determine
	// when the next line is part of the current line group or not until the next line is parsed. Even for the
	// 'continue' mode there is not guarantee that the continued log line will appear soon, if ever.
	// IdleDuration should not be greater than the position sync period (`positions.sync-period`) so no logs are lost if
	// some crash occurs when the position of the first line of the multiline log is sync to disk*.
	// The IdleDuration is calculated from the time the first line of the multiline log is added and not updates for each new
	// log line appended. The default value is "5s". You can disable the max wait using a zero duration.
	IdleDuration string `yaml:"max_idle_duration"`

	// Separator text is added between lines of the multiline entry, e.g. you can use `delimiter: '\n'` to preserve
	// line breaks on the entry. The default delimiter is empty.
	Separator string `yaml:"separator"`
}

type multiLineParser struct {
	// modeHandler with specific parsing instructions. There is a handler for each parsing `Config Mode`.
	modeHandler func(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) error

	// compiled regexp for `Config Expression`
	expressionRegex *regexp.Regexp

	// compiled regexp for `Config FirstLineExpression`
	firstLineRegex *regexp.Regexp

	// compiled regexp for `Config NextLineExpression`
	nextLineRegex *regexp.Regexp

	// maxWait define the max time a multiline entry will be wait for new lines. It's a go duration parsing of
	// `Config IdleDuration`
	maxWait time.Duration

	// i.e. `Config Separator`
	separator string

	// log with context multiline keyvals
	logger log.Logger

	// multitrack determines if the parser can manage multiple multiline entries at the same time
	multitrack bool

	// multilines is used when `multitrack=true`
	// using a slice instead of a map to preserve the order of the log lines
	// assumed there are only a few group keys for the same time window
	// in the worst case maxWait will release the entries eventually
	// up to ~100 entries it should not be problem to fetch
	multilines []*multilineEntry

	// multilines is used when `multitrack=false`
	multiline *multilineEntry

	// flusher ticker check that tracked multiline log entries not exceeded the max time they can be retained as
	// specified by `maxWait`. Interval is half `maxWait`.
	flusher *time.Ticker

	// next os the entry handler use to handle the parsed multiline log entries
	next api.EntryHandler

	//json parser state
	jsonState int
	//json open branch counter
	jsonObjectCtr int

	// concurrency control for `multilines`, `multiline` and handling entries
	sync.Mutex
}

// multilineEntry manages a multiline log entry
type multilineEntry struct {
	// enrollTime is the time the first log line was set
	enrollTime time.Time
	// entry labels, updated for each log line added
	labels model.LabelSet
	// timestamp of the *first* log line entry. so, the timestamp send to the `next` handler  will  be the timestamp of
	// the first log line
	timestamp time.Time
	// this multiline log entry group key
	key string
	// text of the log lines concatenated
	entry string
	// number of log lines contained in this entry
	lines int
}

func (d *multilineEntry) reset() {
	d.labels = model.LabelSet{}
	d.entry = ""
	d.lines = 0
}

// inits the multiline entry with the values provided. The struct will contain exactly one log line after this call
func (d *multilineEntry) init(labels model.LabelSet, t time.Time, entry string) {
	d.labels = labels.Clone()
	d.timestamp = t
	d.entry = entry
	d.lines = 1
	d.enrollTime = time.Now()
}

// append a line to the multi log line entry and merge the labels
func (d *multilineEntry) append(labels model.LabelSet, entry string, delimiter string) {
	d.labels = labels.Merge(labels)
	d.entry = join(d.entry, delimiter, entry)
	d.lines++
}

func (c *multiLineParser) startFlusher() {
	// set the ticker interval to half the maxWait period to guarantee maxWait period for the entries
	flusher := time.NewTicker(c.maxWait / 2)
	go func() {
		for {
			select {
			case <-flusher.C:
				err := c.flush(false)
				if err != nil {
					level.Debug(c.logger).Log("msg", "failed to flush multiline logs", "err", err)
				}
			}
		}
	}()
	c.flusher = flusher
}

// Flush force continuation to the handler of the retained multiline log entries
func (c *multiLineParser) Flush() error {
	return c.flush(true)
}

// Close the handler. Flush pending entries
func (c *multiLineParser) Stop() error {
	if c.flusher != nil {
		// stop the ticker
		c.flusher.Stop()
	}
	// flush multiline entries
	c.flush(true)
	return nil
}

// check all current multiline entries for time out (maxWait reached)
// if force is set handle all entries even if not time out occurred
func (c *multiLineParser) flush(force bool) error {
	now := time.Now()

	c.Lock()
	var err util.MultiError
	if c.multitrack {
		// a new list is built with the valid entries
		nextGen := make([]*multilineEntry, 0, len(c.multilines))
		// check each multiline entry
		for _, t := range c.multilines {
			// remove multilog entries with no lines
			if t.lines == 0 {
				continue
			}
			// handle entries if forced or it's out of validity range
			if force || now.Sub(t.enrollTime) > c.maxWait {
				err.Add(c.next.Handle(t.labels, t.timestamp, t.entry))
			} else {
				// append the entry to the next gen list if the entry is valid yet
				nextGen = append(nextGen, t)
			}
		}
		// assign the next gen list
		c.multilines = nextGen
	} else {
		t := c.multiline
		if t.lines > 0 && (force || now.Sub(t.enrollTime) > c.maxWait) {
			err.Add(c.next.Handle(t.labels, t.timestamp, t.entry))
			// reuse struct
			t.reset()
		}
	}
	c.Unlock()

	return err.Err()
}

// NewMultiLineParser construct a new multiline parser
func NewMultiLineParser(logger log.Logger, config *Config, next api.EntryHandler) (EntryHandler, error) {
	if config == nil {
		return nil, errors.New(ErrEmptyMultiLineConfig)
	}
	ml := &multiLineParser{}

	// log config
	if logger == nil {
		logger = log.NewNopLogger()
	} else {
		ml.logger = log.With(logger, "component", "multiline")
	}

	// expression config
	exp := config.Expression
	if len(exp) == 0 {
		exp = "^.*$"
	}
	expr, err := regexp.Compile(exp)
	if err != nil {
		return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineExpressionRegex)
	}
	ml.expressionRegex = expr

	// first line expression config
	if len(config.FirstLineExpression) > 0 {
		expr, err := regexp.Compile(config.FirstLineExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineFirstLineRegex)
		}
		ml.firstLineRegex = expr
	}

	// next line expression config
	if len(config.NextLineExpression) > 0 {
		expr, err := regexp.Compile(config.NextLineExpression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineNextLineRegex)
		}
		ml.nextLineRegex = expr
	}

	// max wait config
	ml.maxWait = 5 * time.Second
	if config.IdleDuration != "" {
		var err error
		ml.maxWait, err = time.ParseDuration(config.IdleDuration)
		if err != nil {
			return nil, errors.Wrap(err, ErrMultiLineUnvalidMaxWaitTime)
		}
	}

	// separator config

	ml.separator = config.Separator

	// mode and multitrack config
	// and determine if maxWait is required
	requireMaxWait := false

	switch config.Mode {
	case "newline":
		ml.modeHandler = handleNewLineMode
	case "group":
		ml.modeHandler = handleGroupMode
		requireMaxWait = true
	case "unordered_group":
		ml.modeHandler = handleUnorderedGroupMode
		ml.multitrack = true
		requireMaxWait = true
	case "continue":
		ml.modeHandler = handleContinueMode
	case "json":
		ml.modeHandler = handleJsonMode
		requireMaxWait = true
	default:
		return nil, errors.New(ErrMultiLineUnsupportedMode)
	}

	if ml.multitrack {
		ml.multilines = make([]*multilineEntry, 0, 7)
	} else {
		ml.multiline = newMultiLineEntry("")
	}

	// next handler config
	if next == nil {
		level.Warn(ml.logger).Log("msg", "multiline next handler is not defined")
		next = api.EntryHandlerFunc(func(labels model.LabelSet, time time.Time, entry string) error {
			return nil
		})
	}

	ml.next = next

	// post config

	//start flusher if required
	if ml.maxWait > 0 {
		ml.startFlusher()
	} else if requireMaxWait {
		return nil, errors.New(ErrMultiLineModeRequireMaxWait)
	} else {
		level.Warn(ml.logger).Log("msg", "multiline flusher disabled")
	}

	return ml, nil
}

// Multiline entry handler
func (c *multiLineParser) Handle(labels model.LabelSet, t time.Time, entry string) (err error) {
	// labels should not be nil, never
	if labels == nil {
		labels = model.LabelSet{}
	}
	c.Lock()
	// use mode handler to handle the entry
	err = c.modeHandler(c, labels, t, entry)
	c.Unlock()
	return
}

// fetchLine returns the multiline entry for the spcified `key`
// a new entry is created if there is no such entry
// so this function never returns nil
func (c *multiLineParser) fetchLine(key string) *multilineEntry {
	for _, t := range c.multilines {
		if t.key == key {
			return t
		}
	}
	ml := newMultiLineEntry(key)
	c.multilines = append(c.multilines, ml)
	return ml
}

// make a new multiline entry properly initialized
func newMultiLineEntry(key string) *multilineEntry {
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

// concat the capturing groups in the parsed regular expression of `s` in `sel`
// concat the rest of the text (i.e. not including the capturing groups) in `inv`
// returns both strings `sel` and `inv`
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

// selection return the concatenation of the capturing groups of the regular expression of `s` when `expression != nil`
// return the string `s` unaltered otherwise
func selection(expression *regexp.Regexp, s string) string {
	if expression == nil {
		return s
	}
	sel, _ := disjoint(expression, s)
	return sel
}

func selectionDynamic(c *multiLineParser, ml *multilineEntry, line string) string {
	//select regexp based in prev multiline content
	expr := c.firstLineRegex
	if ml.entry != "" {
		expr = c.nextLineRegex
	}
	return selection(expr, line)
}
