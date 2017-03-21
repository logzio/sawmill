# Sawmill
Log enricher an transformer TBD

Simple configuration example:
```json
{
  "steps": [
    {
      "grok": {
        "name": "grok message",
        "config": {
          "field": "message",
          "overwrite": [
            "message"
          ],
          "patterns": [
            "(%{IPORHOST:client_ip}|-) %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \\\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\\\" %{NUMBER:response:int} (?:%{NUMBER:bytes:float}|-) B %{DATA:thread} %{NUMBER:response_time:float} ms %{DATA:servername} %{DATA:client_id:int}(\\;%{NOTSPACE})? %{DATA:device_id} %{DATA}"
          ]
        }
      }
    },
    {
      "removeField": {
        "name": "remove message field after grok",
        "config": {
          "path": "message"
        }
      }
    }
  ]
}
```
## NOTE: The Global Pipeline Disables Logstash, so do not use it unless there are no LS configs

## Processors

- grok [grok]
	- field
        - patterns [array]
        - overwrite [array]
	
Example:
```json
{
  "grok": {
    "config": {
      "field": "message",
      "patterns": [
        "^%{WORD:log_level}  ?\\\\[%{TIMESTAMP_ISO8601:timestamp}\\\\] %{NOTSPACE:class}( %{NUMBER:error_code})? %{GREEDYDATA:message}"
      ],
      "overwrite": [
        "message"
      ]
    }
  }
}
```
       
- Add Field [addField]
	- path (the path to the field to add, doted fqdn) 
	- value 
- Append List [appendList]
	- path (the path to the field to add, doted fqdn) 
	- values - array of values to add, i.e. values: ["val1","val2"]
- Add Tag [addTag]
	- tags - array of tags to add, i.e. tags: ["tag1","tag2"]
- Convert Field [convert]
	- path
	- type (one of: long, double, string, boolean)
- Date [date]
	- field 
	- targetField
	- formats - An array,  one of these: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
	- timeZone - one of these: https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html
	
Example: 
```json
{
  "date": {
    "config": {
      "field": "timestamp",
      "targetField": "timestamp",
      "formats": [
        "ISO8601"
      ]
    }
  }
}
```
	
- Drop [drop]
	- percentage, default to 100 which is full drop, can be used to throttle
- Geo IP [geoIp]
	- sourceField
	- targetField
	- tagsOnSuccess [array]
	
Example:
```json
{
  "geoip": {
    "config": {
      "sourceField": "ip",
      "targetField": "geoip",
      "tagsOnSuccess": [
        "geo-ip"
      ]
    }
  }
}
```
   
- Json [json]
	- field
	- targetField
	
Example:

```json
{
  "json": {
    "config": {
      "field": "message",
      "targetField": "json"
    }
  }
}
```
	
- Key Value [kv]
	- field
	- targetField
	- includeKeys - [array]
	- excludeKeys - [array]
	- trim
	- trimKey
	- valueSplit
	- fieldSplit
	- allowDuplicateValues
	
- Remove Field [removeField]
	- path (dotted path, i.e: a.b.c)
	
Example:
   
```json
{
  "removeField": {
    "config": {
      "path": "timestamp"
    }
  }
}
```
   
- Remove Tag [removeTag]
	- tags - list of tags - [array]
- Rename Field [rename]
	- from - the field name to rename
	- to - the new name of that field
- Substitue [gsub]
	- field
	- pattern
	- replacement
- User Agent [userAgent]
	- field
	- targetField
	- prefix
- Split [split]
        - field
	- separator
  
# If Conditions

## Operators
- and [array]
- or [array]
- not [array]

## Conditions
- in
	- path
	- value
- hasValue
	- field
	- possibleValues [array]
- matchRegex
	- field
	- pattern
	- caseInsensitive - default false
	- matchPartOfValue - default false
- exists
	- field
	
Example:
   
 Simple If statement:
 
```json
{
  "if": {
    "condition": {
      "hasValue": {
        "field": "tags"
      },
      "then": [
        {
          "removeTag": {
            "config": {
              "tags": [
                "_jsonparsefailure"
              ]
            }
          }
        }
      ]
    }
  }
}
```

Complex If Statement
   
```json
{
  "if": {
    "condition": {
      "and": [{
        "exists": {
          "field": "clientip"
        }
      }, {
        "not": [{
          "hasValue": {
            "field": "clientip",
            "possibleValues": [
              "None",
              ""
            ]
          }
        }]
      }]
    },
    "then": [{
      "geoIp": {
        "name": "geoip",
        "config": {
          "sourceField": "clientip",
          "targetField": "geoip",
          "tagsOnSuccess": [
            "apache-geoip"
          ]
        }
      }
    }]
  }
}
```
	
## Additional Commands
- stopOnFailure [boolean]
    - false (default)  The pipeline will continue through the steps even if there is a processor failure
    - true - The pipeline will stop processing at the first processor that has a failure
    
	

## Open source
In order to move to public repository, these are the items we need to fix:
- Remove the dependency of custom java-grok in Nexus
