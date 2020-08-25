package multiline

import (
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/loki/pkg/util"
	"github.com/prometheus/common/model"
	"strings"
	"time"
)

const (
	jsonRegularInvalidState = iota
	jsonInDocState
	jsonInStringState
)

// Handler for json mode. Lines are valid json documents
// note: ugly, simplify
func handleJsonMode(c *multiLineParser, labels model.LabelSet, t time.Time, entry string) error {
	// json mode handler is not multi tracked
	ml := c.multiline

	var err util.MultiError

out:
	for {
		if entry == "" {
			break
		}
		//very very relaxed "json" parser
		switch c.jsonState {
		case jsonRegularInvalidState:
			i := strings.Index(entry, "{")
			if i < 0 {
				//just ignore no json docs
				break out
			} else {
				//ignore prev no json docs
				//init multiline
				ml.init(labels, t, "{")
				entry = entry[i+1:]
				c.jsonObjectCtr = 1
				c.jsonState = jsonInDocState

			}
		case jsonInDocState, jsonInStringState:
			left := ""
			for {
				i := strings.IndexAny(entry, "\\\"{}")
				if i < 0 {
					left += entry
					entry = ""
					break
				}
				l := len(entry)
				switch entry[i] {
				case '\\':
					if i < l {
						i++
					}
				case '"':
					if c.jsonState == jsonInDocState {
						c.jsonState = jsonInStringState
					} else {
						c.jsonState = jsonInDocState

					}
				case '{':
					if c.jsonState != jsonInStringState {
						c.jsonObjectCtr++
					}

				case '}':
					if c.jsonState == jsonInStringState {
						break
					}
					c.jsonObjectCtr--
					if c.jsonObjectCtr == 0 {
						left = left + entry[:i+1]
						ml.append(labels, selectionDynamic(c, ml, left), "")
						err.Add(c.next.Handle(ml.labels, ml.timestamp, ml.entry))
						ml.reset()
						entry = entry[i+1:]
						left = ""
						c.jsonState = jsonRegularInvalidState
						continue out
					}
				}
				left = left + entry[:i+1]
				entry = entry[i+1:]
				if entry == "" {
					break
				}
			}
			if left != "" {
				ml.append(labels, selectionDynamic(c, ml, left), "")
			}
		default:
			//something very wrong here
			level.Warn(c.logger).Log("msg", "invalid json parsing state", "state", c.jsonState, "text", ml.entry+entry)
			ml.reset()
			c.jsonState = jsonRegularInvalidState
			c.jsonObjectCtr = 0
			break out
		}
	}

	if ml.entry != "" {
		ml.append(labels, "", c.separator)
	}

	return err.Err()
}
