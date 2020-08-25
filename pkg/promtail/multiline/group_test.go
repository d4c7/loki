package multiline

import (
	"strings"
	"testing"
)

func TestMultilineGroupMode(t *testing.T) {
	t.Parallel()

	tests := map[string]modeTest{

		"group mode": {
			Config{
				Mode:       "group",
				Expression: `^(\S+)`,
			},
			[]string{`G:1 event`,
				`G:1 one`,
				`G:2 event`,
				`G:2 two`},
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},

		"group mode compound key": {
			Config{
				Mode:       "group",
				Expression: `(G:\S+).*(H:\S+)`,
				Separator:  " ",
			},
			[]string{`1 G:1 event H:2 rest1`,
				`2 G:1 one H:2 rest2`,
				`3 G:2 event H:2 rest3`,
				`4 G:2 two H:2 rest4`},
			[]string{
				"1 G:1 event H:2 rest1 2  one  rest2",
				"3 G:2 event H:2 rest3 4  two  rest4",
			},
			"",
		},

		"java stacktrace": {
			Config{
				Mode:       "newline",
				Expression: `^\[.*] - `,
				Separator:  "\n",
			},
			append(strings.Split(javaLogLine1, "\n"), javaLogLine2),
			[]string{javaLogLine1, javaLogLine2},
			"",
		},
	}

	runModeTest(t, tests)

}
