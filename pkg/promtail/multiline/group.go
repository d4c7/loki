package multiline

import (
	"github.com/prometheus/common/model"
	"time"
)

// Handler for group mode. Lines are appended by the extracted group key of the lines
func handleGroupMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	// group mode handler is not multi tracked
	ml := c.multiline
	// the group key is the concatenation of the capturing groups of the regular expression
	// `inv` is the inverse of `key`
	key, inv := disjoint(c.expressionRegex, entry)
	if ml.key == key {
		// the group key is equal to the previous line, so we're going to append a new line
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
		// init the multiline entry with the log text or capturing groups if first line regular expression is defined
		// overrides previous struct to reduce allocation
		ml.init(labels, t, selection(c.firstLineRegex, entry))
		//update multiline entry group key
		ml.key = key
	}
	return
}
