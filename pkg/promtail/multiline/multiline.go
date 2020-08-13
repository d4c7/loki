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

// FlushableEntryHandler is an api.EntryHandler that allows to flush buffered log lines
type FlushableEntryHandler interface {
	//Flush orders the immediate drain of the log entries retained
	Flush() error

	api.EntryHandler
}

// Note log lines could be lost if MaxWait > positions.sync-period ( def 10*time.Second)
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
	// MaxWait should not be greater than the position sync period (`positions.sync-period`) so no logs are lost if
	// some crash occurs when the position of the first line of the multiline log is sync to disk*.
	// The MaxWait is calculated from the time the first line of the multiline log is added and not updates for each new
	// log line appended. The default value is "5s". You can disable the max wait using a zero duration.
	MaxWait string `yaml:"max_wait"`

	// Delimiter text is added between lines of the multiline entry, e.g. you can use `delimiter: '\n'` to preserve
	// line breaks on the entry. The default delimiter is empty.
	Delimiter string `yaml:"delimiter"`
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
	// `Config MaxWait`
	maxWait time.Duration

	// i.e. `Config Delimiter`
	separator string

	// log with context multiline keyvals
	logger log.Logger

	// multitrack determines if the parser can manage multiple multiline entries at the same time
	multitrack bool

	// multilines is used when `multitrack=true`
	multilines []*multilineEntry

	// multilines is used when `multitrack=false`
	multiline *multilineEntry

	// flusher ticker check that tracked multiline log entries not exceeded the max time they can be retained as
	// specified by `maxWait`. Interval is half `maxWait`.
	flusher *time.Ticker

	// next os the entry handler use to handle the parsed multiline log entries
	next api.EntryHandler

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
	// this multine log entry group key
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
	// labels should not be nil never, preserve only clone when confirmed
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

// append a line to the multi log line entry and merge the labels
func (d *multilineEntry) append(labels model.LabelSet, entry string, delimiter string) {
	d.labels = labels.Merge(labels)
	d.entry = join(d.entry, delimiter, entry)
	d.lines++
}

func (c *multiLineParser) startFlusher() {
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
func NewMultiLineParser(logger log.Logger, config *Config, next api.EntryHandler) (FlushableEntryHandler, error) {
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
	if len(config.Expression) > 0 {
		expr, err := regexp.Compile(config.Expression)
		if err != nil {
			return nil, errors.Wrap(err, ErrCouldNotCompileMultiLineExpressionRegex)
		}
		ml.expressionRegex = expr
	} else {
		return nil, errors.New(ErrCouldMultiLineExpressionRequiredRegex)
	}

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

	// separator config

	ml.separator = config.Delimiter

	// mode and multitrack config
	switch config.Mode {
	case "newline":
		ml.modeHandler = handleNewLineMode
	case "group":
		ml.modeHandler = handleGroupMode
	case "unordered_group":
		ml.modeHandler = handleUnorderedGroupMode
		ml.multitrack = true
	case "continue":
		ml.modeHandler = handleContinueMode
	default:
		return nil, errors.New(ErrMultiLineUnsupportedMode)
	}

	if ml.multitrack {
		ml.multilines = []*multilineEntry{}
	} else {
		ml.multiline = newLine("")
	}

	// next handler config
	if next == nil {
		level.Warn(ml.logger).Log("msg", "multiline next handler is not defined")
		next = api.EntryHandlerFunc(func(labels model.LabelSet, time time.Time, entry string) error {
			return nil
		})
	}

	ml.next = next

	return ml, nil
}

// Handler for newline mode. Lines are appended until a new line regular expression match
func handleNewLineMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	//continue mode handler is not multitrack
	ml := c.multiline

	if !c.expressionRegex.Match([]byte(entry)) {
		// `entry` is not a new line
		// if there is a next line regular expression use it to append the captured text  to the multiline entry
		// if not append `entry` to the multiline entry
		ml.append(labels, selection(c.nextLineRegex, entry), c.separator)
	} else {
		// `entry` is a new line
		// if a previous multiline entry exists (i.e. has lines) then handle it
		if ml.lines > 0 {
			//handle multiline entry content
			err = c.next.Handle(ml.labels, ml.timestamp, ml.entry)
		}
		// init a new multiline entry
		// overrides previous struct to reduce allocation
		ml.init(labels, t, selection(c.firstLineRegex, entry))
	}
	return
}

// Handler for group mode. Lines are appended by the extracted group key of the lines
func handleGroupMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	// group mode handler is not multitrack
	ml := c.multiline
	// extract the group key `key` and the text minus the group key `inv `from the line
	key, inv := disjoint(c.expressionRegex, entry)
	if ml.key == key {
		// the group key i.e. to the previous line, so we're going to append a new line
		// the default line to appended is the line without the group key to avoid repetition
		line := inv
		// however if there is a next line regular expression the text to append is the capturing groups of the
		// regular expression
		if c.nextLineRegex != nil {
			line = selection(c.nextLineRegex, entry)
		}
		//append the line
		ml.append(labels, line, c.separator)
	} else {
		// the group key is not equal to the previous line
		// handle the previous multiline entry if there is any
		if ml.lines > 0 {
			err = c.next.Handle(ml.labels, ml.timestamp, ml.entry)
		}
		// init a new multiline entry with the log text or capturing groups if first line regular expression is defined
		// overrides previous struct to reduce allocation
		ml.init(labels, t, selection(c.firstLineRegex, entry))
		//update multiline entry group key
		ml.key = key
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
		d.append(labels, line, c.separator)
	} else {
		d.init(labels, t, selection(c.firstLineRegex, entry))
		d.key = key
	}
	return
}

func handleContinueMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	d := c.multiline
	line := selection(c.expressionRegex, entry)
	if line != "" {
		if d.lines > 0 {
			d.append(labels, selection(c.nextLineRegex, line), c.separator)
		} else {
			d.init(labels, t, selection(c.firstLineRegex, line))
		}
	} else {
		if d.lines > 0 {
			d.append(labels, selection(c.nextLineRegex, entry), c.separator)
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
	err = c.modeHandler(c, labels, t, entry)
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
