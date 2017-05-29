# Sawmill
Log enricher an transformer TBD

Simple configuration example:
```json
{
  "steps": [
    {
      "grok": {
        "config": {
          "field": "message",
          "overwrite": [
            "message"
          ],
          "patterns": [
            "(%{IPORHOST:client_ip}|-) %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \\\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion:float})?|%{DATA:rawrequest})\\\" %{NUMBER:response:int} (?:%{NUMBER:bytes:float}|-) B %{DATA:thread} %{NUMBER:response_time:float} ms %{DATA:servername} %{DATA:client_id:int}(\\;%{NOTSPACE})? %{DATA:device_id} %{DATA}"
          ]
        }
      }
    },
    {
      "removeField": {
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
	- ignoreMissing [boolean default = true  means that if the field is missing this is considered successful]
	
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
       
- Add Field [addField] - Can also be used to "replace" a field value - supports "templates"
	- path (the path to the field to add, doted fqdn) 
	- value 
- Append List [appendList] - Supports "templates"
	- path (the path to the field to add, doted fqdn) 
	- values - array of values to add, i.e. values: ["val1","val2"]
- Add Tag [addTag] - Supports "templates"
	- tags - array of tags to add, i.e. tags: ["tag1","tag2"]
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
  "geoIp": {
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
	- prefix
	
- Remove Field [removeField] - Supports "templates"
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
   
- Remove Tag [removeTag] - Supports "templates"
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
- LowerCase [lowerCase]
	- field
- UpperCase [upperCase]
	- fields [array]
- Strip [strip]
	- fields [array]
	
- Convert [convert] must provide exactly one of path or paths
	- path
	- paths - a list of strings representing paths.
	- type - one of [int,long,float,double,string,boolean]

- CSV [csv]
	- field
	- targetField
	- separator
	- quoteChar
	- columns [array]
	- autoGenerateColumnNames [boolean - default = true]
	- skipEmptyColumns [boolean - default = false]
	- convert [this is a json object with key:values where the key is the field and the value is the type as specified in the Convert processor]  EG:
```json
{
  "steps": [
    {
      "csv": {
        "config": {
          "field": "csv",
          "separator": ",",
          "columns": [
            "field1",
            "field2",
            "field3",
            "field4"
          ],
          "convert": {
            "field1": "long",
            "field2": "boolean"
          }
        }
      }
    }
  ]
}
```	
- appendList [appendList]
	- path
	- values [array]

- anonymize [anonymize]
	- fields [array]
	- algorithm
	- key

  
# If Conditions

## Operators
- and [array]
- or [array]
- not [array]

## Conditions
- in  (this is used to find a value in a field that is an array like tags)
	- path
	- value
- hasValue (this finds a match of a string field)
	- field
	- possibleValues [array]
- matchRegex
	- field
	- regex
	- caseInsensitive - default false
	- matchPartOfValue - default true
- exists
	- field
- fieldType
	- path
	- type (One of the following: string,long, double, list, jsonObject)
- mathComparator (can be used to check if a value is grater/smaller then, or a value is in a range)
	- field
	- gt (greater than)
	- gte (greater than or equal to)
	- lt (less than)
	- lte (less than or equal to)
	
Example:
   
 Simple If statement:
 
```json
{
  "if": {
    "condition": {
      "hasValue": {
        "field": "tags",
	"possibleValues":[
	  "_jsonparsefailure"
	]
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

- Templates is the ability to add data from other fields to a new field name or value.  
	- You can call the value of another field using "mustache" syntax  EG:  {{field_name}}
	- Date template could be used to put the current date in a desired format
	- The below example is how to add a field called "timestamp" with the previous values of the "date" and "time" fields, and the current year
```json
    {
      "addField": {
        "config": {
          "path": "timestamp",
          "value": "{{date}} {{time}} {{#dateTemplate}}yyyy{{/dateTemplate}}"
        }
      }
    }

```

    
	

## Open source
In order to move to public repository, these are the items we need to fix:
- Remove the dependency of custom java-grok in Nexus
