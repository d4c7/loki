package multiline

import (
	"regexp"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

type collectHandler struct {
	lines []string
}

func (s *collectHandler) Handle(_ model.LabelSet, _ time.Time, entry string) error {
	s.lines = append(s.lines, entry)
	return nil
}

type modeTest struct {
	config        Config
	logLines      []string
	expectedLines []string
	err           string
}

func runModeTest(t *testing.T, tests map[string]modeTest) {
	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ch := collectHandler{}

			testData.config.IdleDuration = "1000s"
			pl, err := NewMultiLineParser(util.Logger, &testData.config, &ch)
			if err != nil {
				if testData.err != err.Error() {
					t.Fatal(err)
				}
				return
			}

			ls := model.LabelSet{}
			ts := time.Now()
			for _, s := range testData.logLines {
				err = pl.Handle(ls, ts, s)
				if err != nil {
					t.Failed()
				}
			}
			err = pl.Stop()
			if err != nil {
				t.Failed()
			}

			for i, n, cl := 0, len(testData.expectedLines), len(ch.lines); i < n; i++ {
				if i >= cl {
					assert.Fail(t, "<missing line> '"+testData.expectedLines[i]+"'")
				} else {
					assert.Equal(t, testData.expectedLines[i], ch.lines[i])
				}
			}

			for i, n := len(testData.expectedLines), len(ch.lines); i < n; i++ {
				assert.Fail(t, "<unexpected line> '"+ch.lines[i]+"'")
			}
		})
	}
}

func TestMultilineTimeout(t *testing.T) {
	cfg := Config{
		Mode:         "continue",
		Expression:   `(.*)\\$`,
		IdleDuration: "10ms",
	}
	logLines := []string{
		`event \`,
		`one\`,
	}
	ch := collectHandler{}

	pl, err := NewMultiLineParser(util.Logger, &cfg, &ch)
	if err != nil {
		t.Fatal(err)
	}

	ls := model.LabelSet{}
	ts := time.Now()
	for _, s := range logLines {
		err = pl.Handle(ls, ts, s)
		if err != nil {
			t.Failed()
		}
	}
	time.Sleep(100 * time.Millisecond)

	if len(ch.lines) != 1 {
		t.Fail()
	} else {
		assert.Equal(t, ch.lines[0], "event one")
	}

}

func TestMultilineMultiTrackTimeout(t *testing.T) {
	cfg := Config{
		Mode:         "group",
		Expression:   `(K:\S+)`,
		IdleDuration: "10ms",
	}
	logLines := []string{
		`K:1 line1`,
		`K:2 line2`,
		`K:3 line3`,
	}
	ch := collectHandler{}

	pl, err := NewMultiLineParser(util.Logger, &cfg, &ch)
	if err != nil {
		t.Fatal(err)
	}

	ls := model.LabelSet{}
	ts := time.Now()
	for _, s := range logLines {
		time.Sleep(15 * time.Millisecond)
		err = pl.Handle(ls, ts, s)
		if err != nil {
			t.Failed()
		}
	}
	if len(ch.lines) != 2 {
		t.Fatal("no 2 lines")
	} else {
		assert.Equal(t, ch.lines[0], "K:1 line1")
		assert.Equal(t, ch.lines[1], "K:2 line2")
	}
	time.Sleep(30 * time.Millisecond)
	if len(ch.lines) != 3 {
		t.Fatal("no 3 lines")
	} else {
		assert.Equal(t, ch.lines[2], "K:3 line3")
	}
}

func TestMultilineDisjoint(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		regexp        string
		entry         string
		expectedSel   string
		expectedUnSel string
	}{
		"t1": {
			`F:(\S+\s*)`,
			"F:1 F:2",
			"1 2",
			"F:F:"},

		"t2": {
			`(F:\S+\s*).*(H:\S+\s*)`,
			"E:1 F:1 G:1 H:1",
			"F:1 H:1",
			"E:1 G:1 "},

		"t3": {
			`(F:\S+\s*)+|(H:\S+\s*)+`,
			"E:1 F:1 G:1 H:1 E:2 F:2 G:2 H:2",
			"F:1 H:1 F:2 H:2",
			"E:1 G:1 E:2 G:2 "},

		"t4": {
			`((F:\S+\s*).*(H:\S+\s*))*`,
			"E:1 F:1 G:1 H:1 I:1 E:2 F:2 G:2 H:2 I:2",
			"F:1 G:1 H:1 I:1 E:2 F:2 G:2 H:2 ",
			"E:1 I:2"},
		"continue example": {
			`(.*)\\$`,
			`this line continue\`,
			"this line continue",
			`\`},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			exp, err := regexp.Compile(testData.regexp)
			if err != nil {
				t.Fatal(err)
			}
			sel, inv := disjoint(exp, testData.entry)
			assert.Equal(t, testData.expectedSel, sel)
			assert.Equal(t, testData.expectedUnSel, inv)
		})
	}
}
