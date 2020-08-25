package multiline

import (
	"testing"
)

func TestMultilineContinueMode(t *testing.T) {
	t.Parallel()

	tests := map[string]modeTest{
		"continuation mode": {
			Config{
				Mode:       "continue",
				Expression: `(.*)\\$`,
				Separator:  " ",
			},
			[]string{
				`event\`,
				`one`,
				`two`,
				`event\`,
				`three`},
			[]string{
				"event one",
				"two",
				"event three",
			},
			"",
		},

		"continuation mode handling prefix": {
			Config{
				Mode:               "continue",
				Expression:         `(.*)\\$`,
				NextLineExpression: `BLA.\s(.*)$`,
				Separator:          " ",
			},
			[]string{
				`BLA1 event\`,
				`BLA1 one`,
				`BLA2 two`,
				`BLA3 event\`,
				`BLA3 three`},
			[]string{
				"BLA1 event one",
				"BLA2 two",
				"BLA3 event three",
			},
			"",
		},
	}

	runModeTest(t, tests)

}
