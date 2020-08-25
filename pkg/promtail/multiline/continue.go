package multiline

import (
	"github.com/prometheus/common/model"
	"time"
)

// Handler for continue mode. Lines are appended to the next if a continuation regular expression match the line
func handleContinueMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) (err error) {
	// group mode handler is not multi tracked
	ml := c.multiline
	//select the capturing text for the expression regex
	line := selection(c.expressionRegex, entry)
	if line != "" {
		// the line has a continuation mark
		if ml.lines > 0 {
			// there is a previous multiline entry so append text
			ml.append(labels, selection(c.nextLineRegex, line), c.separator)
		} else {
			// if there is not a previous multiline entry so init one
			ml.init(labels, t, selection(c.firstLineRegex, line))
		}
	} else {
		// the line has not a continuation mark
		if ml.lines > 0 {
			// there is a previous multiline entry so append the text
			ml.append(labels, selection(c.nextLineRegex, entry), c.separator)
			// and handle it
			err = c.next.Handle(ml.labels, ml.timestamp, ml.entry)
			// reset multiline entry
			ml.reset()
		} else {
			// there is not a previous multiline entry and this line has no continuation mark
			// so handle it directly
			err = c.next.Handle(labels, t, entry)
		}
	}
	return
}
