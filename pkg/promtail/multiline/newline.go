package multiline

import (
	"github.com/prometheus/common/model"
	"time"
)

// Handler for newline mode. Lines are appended until a new line regular expression match
func handleNewLineMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	//continue mode handler is not multi tracked
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
