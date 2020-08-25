package multiline

import (
	"github.com/prometheus/common/model"
	"time"
)

// Handler for unordered group mode. Lines are appended by the extracted group key of the lines tracking multiple keys
func handleUnorderedGroupMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	// the group key is the concatenation of the capturing groups of the regular expression
	// `inv` is the inverse of `key`
	key, inv := disjoint(c.expressionRegex, entry)
	// unordered group mode handler is multi tracked
	// fetch the multiline entry of the line group key
	// note: if there is not a multiline entry for the key a new one is created
	ml := c.fetchLine(key)
	if ml.lines > 0 {
		// there is previous log lines for the group key so append the new line
		// the default line to appended is the line without the group key to avoid repetition
		line := inv
		// however if there is a next line regular expression the text to append is the capturing groups of the
		// regular expression
		if c.nextLineRegex != nil {
			line = selection(c.nextLineRegex, entry)
		}
		// append the new line
		ml.append(labels, line, c.separator)
	} else {
		// init the multiline entry with the log text or capturing groups if first line regular expression is defined
		ml.init(labels, t, selection(c.firstLineRegex, entry))
		//set the multiline entry group key
		ml.key = key
	}
	return
}
