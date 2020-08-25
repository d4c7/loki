package multiline

import (
	"strings"
	"testing"
)

const complexJson1 = `{
    "_id": "5f43ade35a23bc7ef7b12ff8",
    "index": 0,
    "isActive": false,
    "balance": "$3,045.99",
    "picture": "http://placehold.it/32x32",
    "name": {
      "first": "Johanna",
      "last": "Rivera"
    },
    "about": "{\"_id\": \"2\",\"name\": {\"first\": \"Two\"}}",
    "range": [
      0,
      9
    ],
    "friends": [
      {
        "id": 0,
        "name": "Rena Pollard"
      }
    ],
    "favoriteFruit": "strawberry"
  }`

func TestMultilineJsonMode(t *testing.T) {
	t.Parallel()

	tests := map[string]modeTest{

		"json mode base test": {
			Config{
				Mode:      "json",
				Separator: "\n",
			},
			[]string{
				"{",
				"	\"_id\": \"1\",",
				"	\"name\": ",
				"   {",
				"		\"first\": \"{One}\"",
				"	}",
				"  },",
				"  {",
				"	\"_id\": \"2\",",
				"	\"name\": {",
				"		\"first\": \"{Two\"",
				"	}",
				"  }",
			},
			[]string{
				"{\n" +
					"	\"_id\": \"1\",\n" +
					"	\"name\": \n" +
					"   {\n" +
					"		\"first\": \"{One}\"\n" +
					"	}\n" +
					"  }",
				"{\n" +
					"	\"_id\": \"2\",\n" +
					"	\"name\": {\n" +
					"		\"first\": \"{Two\"\n" +
					"	}\n" +
					"  }",
			},
			"",
		},
		"json mode complex docs with a lot of newlines": {
			Config{
				Mode:      "json",
				Separator: "\n",
			},
			append(strings.Split(complexJson1, "\n"), strings.Split(strings.ReplaceAll(complexJson1, "{", "{\n"), "\n")...),
			[]string{
				complexJson1,
				strings.ReplaceAll(complexJson1, "{", "{\n"),
			},
			"",
		},
		"json mode multiple json docs in one line": {
			Config{
				Mode:      "json",
				Separator: "\n",
			},
			[]string{`{"_id": "2","name": {"first": "Two{\"}"}}{"_id": "2","name": {"first": "Two{\"}"}}`},
			[]string{
				`{"_id": "2","name": {"first": "Two{\"}"}}`,
				`{"_id": "2","name": {"first": "Two{\"}"}}`,
			},
			"",
		},
		"json mode ignoring garbage": {
			Config{
				Mode:      "json",
				Separator: "\n",
			},
			[]string{`garbage1{"_id": "2"}garbage2{"first": "Two"}garbage3`},
			[]string{
				`{"_id": "2"}`,
				`{"first": "Two"}`,
			},
			"",
		},
		"json mode root array processing": {
			Config{
				Mode:      "json",
				Separator: "\n",
			},
			[]string{`[{"_id": "2"},{"first": "Two"}]`},
			[]string{
				`{"_id": "2"}`,
				`{"first": "Two"}`,
			},
			"",
		},
	}

	runModeTest(t, tests)

}
