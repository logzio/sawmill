# Sawmill
Sawmill is an open source Java Library which enable data processing, enrichments, filtering and transformations.

Using Sawmill pipelines you can process documents with groks, geoip, user-agent resolving, add or remove fields/tags and more dynamically inside your JVM based application.

# Documentation
The full sawmill documentation [can be found here](https://github.com/logzio/sawmill/wiki).

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

## Processors

- grok [grok]
	- field 
	- patterns [array] 
	- overwrite [array] (optional)
	- ignoreMissing [boolean default = true  means that if the field is missing this is considered successful] (optional)
	
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
	- path (the path to the field to add, doted fqdn) (required)
	- value  (required)
- Append List [appendList] - Supports "templates"
	- path (the path to the field to add, doted fqdn)  
	- values - array of values to add, i.e. values: ["val1","val2"] 
- Add Tag [addTag] - Supports "templates"
	- tags - array of tags to add, i.e. tags: ["tag1","tag2"] 
- Date [date]
	- field 
	- targetField (optional)
	- formats - An array,  one of these: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
	- timeZone - one of these: https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html  (optional)  You can try to use this timezone list http://joda-time.sourceforge.net/timezones.html but make sure to test because not all zones exist in sawmill.
	
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
	- tagsOnSuccess [array] (optional)
	
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
	- targetField  (optional)
	
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
	- includeKeys - [array] (optional - but recommended)
	- excludeKeys - [array] (optional)
	- trim  (optional)
	- trimKey (optional)
	- valueSplit (optional - default is = )
	- fieldSplit (optional - default is " ")
	- allowDuplicateValues (optional = default false)
	- prefix (optional)
	
- Remove Field [removeField] - Supports "templates"
	- path - (dotted path, i.e: a.b.c)
	- fields - array of fields to remove, i.e. ["timestamp","message"]
	
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
	- targetField (optional)
	- prefix (optional)
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
	- paths - a list of strings representing paths.  (optional - either use path or paths depending on if you want to convert multiple values or one)
	- type - one of [int,long,float,double,string,boolean]

- CSV [csv]
	- field
	- targetField (optional)
	- separator (optional)
	- quoteChar (optional)
	- columns [array] (optional)
	- autoGenerateColumnNames [boolean - default = true]
	- skipEmptyColumns [boolean - default = false]
	- convert [this is a json object with key:values where the key is the field and the value is the type as specified in the Convert processor] (optional)  EG:
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
- translate [translate]
	- field
	- targetField
	- fallback (optional)
	- dictionary [this is a json object with key:values where the key is the original value, and the value is what you want to replace it with] EG:
	
```json
{
  "steps": [
    {
      "translate": {
        "config": {
          "field": "username",
          "targetField": "username_translated",
          "fallback": "unknown",
          "dictionary": {
            "josh": "josh_theking",
            "joshphillips": "josh_squared",
            "joshledger": "josh_cubed"
          }
        }
      }
    }
  ]
}
```
- math [math]
	- targetField
	- expression

- ahoCorasick
    - field
    - targetField
    - inputWords []
    
- xml [xml]
	- field 
	- targetField
	- storeXml [boolean, default = true]
	- xpath [this is a json object with key:values where the key is the xpath, and the value is the field name you want for that path]  EX:
	
```json
{
  "steps": [
    {
      "xml": {
        "config": {
          "field": "message",
          "targetField": "xmldata",
          "xpath": {
            "/testng-results/suite/test/@started-at": "start_time",
            "/testng-results/suite/test/@finished-at": "finish_time",
            "/testng-results/suite/test/@duration-ms": "total_time"
          }
        }
      }
    }
  ]
}
```
  
# If Conditions

## Operators
- and [array]
- or [array]
- not [array]

## Conditions
- in  (this is used to find a value in a field that is an array like tags)
	- path
	- value
- hasValue (checks if a field has a value)
	- field
	- possibleValues [array]  (type sensitive, if it's a number make sure to have the value not in quotes, if it's a string have it in quotes, if it can be both be safe and have the values both in quotes and not in quotes.
- matchRegex (only works on string fields)
	- field
	- regex
	- caseInsensitive - default false (optional)
	- matchPartOfValue - default true (optional)
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
        "field": "level",
	"possibleValues":[
	  "INFO"
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

