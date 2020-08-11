package multiline

import (
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

//group_source ??
func TestCoalesce(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config        Config
		logLines      string
		expectedLines []string
		err           string
	}{
		"simple line continuation": {
			Config{
				ContinueLineExpression: "\\\\$",
				JoinText:               " ",
			},
			`event\
one
event\
two`,
			[]string{
				"event one",
				"event two",
			},
			"",
		},

		"indented sublines": {
			Config{
				GroupKeyExpression: "^[^ ]",
			},
			`line 1
 subline 1.1
 subline 1.2
line 2
 subline 2.1
ignore line 3`,
			[]string{
				"line 1 subline 1.1 subline 1.2",
				"line 2 subline 2.1",
			},
			"",
		},

		"group id with line continuation multitrack": {
			Config{
				GroupKeyExpression:     "^G:(\\d) *",
				ContinueLineExpression: "\\\\$",
				JoinText:               " ",
				MultiTrack:             true,
			},
			`G:1 event\
G:2 event\
G:1 one
G:2 two`,
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},

		"append expression with line continuation": {
			Config{
				ContinueLineExpression: "\\\\$",
				AppendExpression:       "^\\S+\\s(.*)$",
			},
			`G:1 event \
G:1 one
G:2 event \
G:2 two`,
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},

		"append expresion with group lines": {
			Config{
				GroupKeyExpression: "^(\\S+)",
				AppendExpression:   "^\\S+\\s(.*)$",
				JoinText:           " ",
			},
			`G:1 event
G:1 one
G:2 event
G:2 two
G:3 ignore this line`,
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			//	t.Parallel()

			ch := collectHandler{}

			pl, err := NewMultiLineParser(util.Logger, &testData.config, &ch)
			if err != nil {
				if testData.err != err.Error() {
					t.Fatal(err)
				}
				return
			}

			lbls := model.LabelSet{}
			ts := time.Now()
			for _, s := range strings.Split(testData.logLines, "\n") {
				err = pl.Handle(lbls, ts, s)
				if err != nil {
					t.Failed()
				}
			}

			for i, n, cl := 0, len(testData.expectedLines), len(ch.lines); i < n; i++ {
				var b string
				if i >= cl {
					b = "<missing>"
				} else {
					b = ch.lines[i]
				}
				assert.Equal(t, testData.expectedLines[i], b)
			}
			assert.Equal(t, len(testData.expectedLines), len(ch.lines))

		})
	}
}

type collectHandler struct {
	lines []string
}

func (s *collectHandler) Handle(labels model.LabelSet, t time.Time, entry string) error {
	s.lines = append(s.lines, entry)
	return nil
}
