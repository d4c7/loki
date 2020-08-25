package multiline

import (
	"strings"
	"testing"
)

const (
	pythonLogLine1 = `[2019-08-13 06:58:20,588] ERROR in app: Exception on /graphql [POST]
Traceback (most recent call last):
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 2292, in wsgi_app
    response = self.full_dispatch_request()
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 1815, in full_dispatch_request
    rv = self.handle_user_exception(e)
AttributeError: 'Exception' object has no attribute 'path'`

	pythonLogLine2 = `[2019-08-13 06:58:20,589] INFO bla`

	javaLogLine1 = `[2019-08-13 22:00:12 GMT] - [main] ERROR c.i.b.w.w.WebAdapterAgent: cycle failed:
java.lang.NumberFormatException: For input string: "-db error"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Integer.parseInt(Integer.java:580)
Caused by: MidLevelException: LowLevelException
	at Junk.a(Junk.java:11)
	... 1 more`
	javaLogLine2 = `[2019-08-13 22:00:13 GMT] - [main] INFO  c.i.b.w.w.WebAdapterAgent: All services are now up and running`

	aptHistoryLogLine1 = `Start-Date: 2020-05-15  14:46:48
Commandline: /usr/bin/apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold install docker-ce
Install: containerd.io:amd64 (1.2.13-2, automatic), docker-ce:amd64 (5:19.03.8~3-0~ubuntu-bionic), docker-ce-cli:amd64 (5:19.03.8~3-0~ubuntu-bionic, automatic)
End-Date: 2020-05-15  14:47:04`

	aptHistoryLogLine2 = ``

	aptHistoryLogLine3 = `Start-Date: 2020-05-16  06:06:29
Commandline: /usr/bin/unattended-upgrade
Upgrade: apt-transport-https:amd64 (1.6.12, 1.6.12ubuntu0.1)
End-Date: 2020-05-16  06:06:30`
)

func TestMultilineNewLineMode(t *testing.T) {
	t.Parallel()

	tests := map[string]modeTest{

		"newline mode": {
			Config{
				Mode:       "newline",
				Expression: "^[^ ]",
			},
			[]string{
				`line 1`,
				` subline 1.1`,
				` subline 1.2`,
				`line 2`,
				` subline 2.1`},
			[]string{
				"line 1 subline 1.1 subline 1.2",
				"line 2 subline 2.1",
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
		"python stacktrace": {
			Config{
				Mode:       "newline",
				Expression: `^\[.*]`,
				Separator:  "\n",
			},
			append(strings.Split(pythonLogLine1, "\n"), pythonLogLine2),
			[]string{pythonLogLine1, pythonLogLine2},
			"",
		},
		"apt log history": {
			Config{
				Mode:       "newline",
				Expression: `^$`,
				Separator:  "\n",
			},
			append(append(strings.Split(aptHistoryLogLine1, "\n"), aptHistoryLogLine2), strings.Split(aptHistoryLogLine3, "\n")...),
			[]string{aptHistoryLogLine1, aptHistoryLogLine3},
			"",
		},

		"named line as separator": {
			Config{
				Mode:                "newline",
				Expression:          `^SEP$`,
				FirstLineExpression: `^$`, // remove first line
				Separator:           "\n",
			},
			[]string{
				"line A-1",
				"line A-2",
				"SEP",
				"line B-1",
				"line B-2",
			},
			[]string{
				"line A-1\nline A-2",
				"line B-1\nline B-2",
			},
			"",
		},
		"json serendipity": {
			Config{
				Mode:       "newline",
				Expression: `^\s*\{\s*$`,
				Separator:  "\n",
			},
			[]string{
				"{",
				"	\"_id\": \"1\",",
				"	\"name\": {",
				"		\"first\": \"One\",",
				"	}",
				"  },",
				"  {",
				"	\"_id\": \"2\",",
				"	\"name\": {",
				"		\"first\": \"Two\",",
				"	}",
				"  }",
			},
			[]string{
				"{\n" +
					"	\"_id\": \"1\",\n" +
					"	\"name\": {\n" +
					"		\"first\": \"One\",\n" +
					"	}\n" +
					"  },",
				"  {\n" +
					"	\"_id\": \"2\",\n" +
					"	\"name\": {\n" +
					"		\"first\": \"Two\",\n" +
					"	}\n" +
					"  }",
			},
			"",
		},
	}

	runModeTest(t, tests)

}
