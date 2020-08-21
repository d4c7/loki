---
title: script
---
# `script` stage


WARN: WIP

The `script` stage is a scripting stage that allows for complex behaviour that
can not be easly done with other pipeline stages. The source language is [Tengo](https://github.com/d5/tengo).

You can read and write labels, sources, timestamp and entry.

Write a read labels:

```tengo
 label["l1"]="v1"

 if label["l2"]=="" { 
    label["l2"]="v2"
 }
```

Write a read source:

```tengo
 source["s1"]="v1"

 if source["s2"]=="" { 
    source["s2"]="v2"
 }
```

You can maintain state using state variables defined in the config.


## Schema

```yaml

script:
  text: <string>
  state: <map>
  [debug: <bool>]
```


### Examples

```yaml
- script:
      text:
        h := source["items"]
        
        if h {
    
          l := len(h)
    
          source["items_count"] = l
    
          if l > 1 {
    
            label["type"] = "multi_items"
    
          }
    
        }
```
