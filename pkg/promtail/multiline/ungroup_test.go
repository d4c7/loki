package multiline

import (
	"testing"
)

func TestMultilineUnGroupMode(t *testing.T) {
	t.Parallel()

	tests := map[string]modeTest{

		"group mode unordered": {
			Config{
				Mode:       "unordered_group",
				Expression: `^(\S+)`,
			},
			[]string{`G:1 event`,
				`G:2 event`,
				`G:1 one`,
				`G:2 two`},
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},
	}

	runModeTest(t, tests)

}
