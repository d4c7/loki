---
title: Multiline Parser
---

A detailed look at how to setup Promtail to process multiline log lines.

A complete config file sample:

```yaml
   multiline_parser:
      mode: "newline"
      expression: '^time:'
      first: '^(.*)$'
      next: '^\s+(.*)$' 
      separator: ' '
      max_idle_duration: '5s'
```

The [`mode`](#multiline-modes) defines the global behaviour of the parser. With `expression` you 
 can define the regular expression that extracts (capturing groups) the text to append to the multiline log.
 [`first`](#first-and-next-expressions)  and 
 [`next`](#first-and-next-expressions) 
 allows to refine the text extracted from the first and next log lines.
  [`separator`](#separator) allows you to specify the
 text used to join the log lines. [`max_idle_duration`](#max-idle-duration) limit the time a multilog line cab be hold by the parser.
  Only `mode`and `expression` are required. 
  
  You can view some typical examples [here](#common-examples).

## Max Idle Duration

A `max_idle_duration` config is provided in order to not hold the grouping lines too much: 
- The parser cannot determine when the next line is part of the current line group or not until the next line is parsed.
- For the "Continue Mode" there is not guarantee that the continued log line will appear soon, if ever. 

The `max_idle_duration` must not be greater than the position sync period (`positions.sync-period`). 
*The lines in the grouping  phase could be lost if some crash occurs when the position of the first line of the multiline log is sync to disk*.

The default value for `max_idle_duration` is "5s".  

## First and Next Expressions

Sometimes you need to filter the log lines. You can use `first` regular expresion to select the text to be added as the
first line of the multiline log. You can use `next` regular expression to select the text to be added to the multiline
log. 

## Separator 

The `separator` text is added between lines. So, for example you can use `separator: '\n'` to preserve line breaks. The
default separator is empty.


## Multiline Modes

The parses has defined some modes of operation to ease the use of distinct use cases in which a multiline parser
 is required. This modes are:
 
- [New Line Mode](#new-line-mode): A new multine starts when a "newline" regular expression match.
- [Continue Mode](#continue-mode): A line that match a "continue" regular expresion are joined with the next one.
- [Group Mode](#group-mode): A "group" regular expression is used to determine the group a line belongs to.
- [Unordered Group Mode](#unordered-group-mode): This is a "Group Mode" but tracking multiple group keys simultaneously. So the lines can be
 parsed unordered.
- [JSON Mode](#json-mode): parses multiline json documents

### New Line Mode

In the multiline mode the lines are joined until a new line mark is found. Then the previous collected lines are 
processed as a single one.

#### Example:

Given the following log lines:

```
log line 1
    sub log line 1.1
log line 2
    sub log line 2.1
```

We can define the multiline parser:

```yaml
   multiline_parser:
      mode: "newline"
      expression: '^[^ ]'
```

The following multilog lines are generated:

```
log line 1    sub log line 1.1
log line 2    sub log line 2.1
```

We can use `next` regular expression to remove the prefix spaces and `separator` to separate joined lines with a space:

```yaml
   multiline_parser:
      mode: "newline"
      expression: '^[^ ]",'
      next: '^\s*(.*)$'
      separator: ' '
```
Parsing the previous log now gives:
```
log line 1 sub log line 1.1
log line 2 sub log line 2.1
```

## Continue Mode

A line that match a "continue" regular expresion are joined with the next one.


#### Example 1:

Given the following log lines:

```
log event #
one
log event #
two
```

We can define the multiline parser as:

```yaml
   multiline_parser:
      mode: "continue"
      expression: '^(.*)#$'
```

The following multilog lines are parsed:

```
log event one
log event two
```

#### Example 2:

Given the following log lines:

```
t1: log event \
t1: one
t2: log event \
t2: two
```

We can define the multiline parser as:

```yaml
   multiline_parser:
      mode: "continue"
      expression: '^(.*)\\$'
      next: '^t.: :(.*)$' 
```

The following multilog lines are parsed:

```
t1: log event one
t2: log event two
```

NOTE:
We can use `next` and `prev` regular expressions but remmber the regular expression is applied to the line with the
 selected text and not the full log line.
 
 
## Group Mode

 A "group" regular expression is used to determine the group key for a log line. All the lines with same group key are
 joined.


#### Example 1:

Given the following log lines:

```
request_id:1 log event
request_id:1 one
request_id:2 log event
request_id:2 two
```

We can define the multiline parser as:

```yaml
   multiline_parser:
      mode: "group"
      expression: 'request_id:(\S+)'
```

We are selecting the lines key as the value of the request_id

The following multilog lines are parsed:

```
request_id:1 log event one
request_id:2 log event two
```

NOTE: The group key is keep at the first line but removed for the next lines. You can override this behaviour using 
`next`and `first` regular expressions. 


# Unordered Group Mode
 
Sometimes the log lines are not perfectly ordered. In these cases you can use unordererd group mode to track multiple
log lines and group them together. The parses tries to preserve the order of the log lines as much as possible.


#### Example:

Given the following log lines:

```
request_id:1 log event
request_id:2 two
request_id:2 log event
request_id:1 one
```

We can define the multiline parser as:

```yaml
   multiline_parser:
      mode: "unordered_group"
      expression: 'request_id:(\S+)'
```

We are selecting the lines key as the value of the request_id

The following multilog lines are parsed:

```
request_id:1 log event one
request_id:2 log event two
```




# JSON Mode
 
Sometimes you can use previous modes in order to separate multiple JSON documents. 
However you can use this specific mode for this task.
The JSON mode use a very relaxed json parser so it can handle invalid JSON documents.

Note 1: The text between valid JSON documents is ignored. 

Note 2: Multiple JSON documents in the same line generate as many lines as JSON docs found in the line

#### Example:

Given the following log lines:

```json
{
  "_id": "1"
}
{
  "_id": "2"
}
```

We can define the multiline parser as:

```yaml
   multiline_parser:
      mode: "json"
      separator: "\n"
```

to obtain the next logs lines:

```
#line 1

{
  "_id": "1"
}

#line 2

{
  "_id": "2"
}
```

## Common Examples

## Python logs
 
Python logs:

```
[2019-08-13 06:58:20,588] ERROR in app: Exception on /graphql [POST]
Traceback (most recent call last):
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 2292, in wsgi_app
    response = self.full_dispatch_request()
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 1815, in full_dispatch_request
    rv = self.handle_user_exception(e)
AttributeError: 'Exception' object has no attribute 'path'`
```

Parser config:
```yaml
   multiline_parser:
      mode: "newline"
      expression: '^\[.*] ' 
      separator: '\n
```

## Java logs
 
Java logs with multiline stack traces:

```
[2019-08-13 22:00:11 GMT] - [main] INFO  c.i.b.w.w.WebAdapterAgent: go
[2019-08-13 22:00:12 GMT] - [main] ERROR c.i.b.w.w.WebAdapterAgent: cycle failed:
java.lang.NumberFormatException: For input string: "-db error"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Integer.parseInt(Integer.java:580)
Caused by: MidLevelException: LowLevelException
	at Junk.a(Junk.java:11)
	... 1 more`
[2019-08-13 22:00:13 GMT] - [main] INFO  c.i.b.w.w.WebAdapterAgent: All services are now up and running
```
```yaml
   multiline_parser:
      mode: "newline"
      expression: '^\[.*] -' 
      separator: '\n
```

## Apt History Log

Apt history logs are multiline entries separated by an empty line

```
Start-Date: 2020-05-15  14:46:48
Commandline: /usr/bin/apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold install docker-ce
Install: containerd.io:amd64 (1.2.13-2, automatic), docker-ce:amd64 (5:19.03.8~3-0~ubuntu-bionic), docker-ce-cli:amd64 (5:19.03.8~3-0~ubuntu-bionic, automatic)
End-Date: 2020-05-15  14:47:04

Start-Date: 2020-05-16  06:06:29
Commandline: /usr/bin/unattended-upgrade
Upgrade: apt-transport-https:amd64 (1.6.12, 1.6.12ubuntu0.1)
End-Date: 2020-05-16  06:06:30
```

Suggested config:

```yaml
   multiline_parser:
      mode: "newline"
      expression: '^$' 
      separator: '\n
```

Parsed Log Line 1:
```
Start-Date: 2020-05-15  14:46:48
Commandline: /usr/bin/apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold install docker-ce
Install: containerd.io:amd64 (1.2.13-2, automatic), docker-ce:amd64 (5:19.03.8~3-0~ubuntu-bionic), docker-ce-cli:amd64 (5:19.03.8~3-0~ubuntu-bionic, automatic)
End-Date: 2020-05-15  14:47:04
```

Parsed Log Line 2:
```
Start-Date: 2020-05-16  06:06:29
Commandline: /usr/bin/unattended-upgrade
Upgrade: apt-transport-https:amd64 (1.6.12, 1.6.12ubuntu0.1)
End-Date: 2020-05-16  06:06:30
```

