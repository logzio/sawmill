# Sawmill
Log enricher an transformer TBD

Simple configuration example:
```
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

## Processors


Processors:
- Add Field
	- path (the path to the field to add, doted fqdn) 
	- value 
- Add Tag
	- tags - array of tags to add, i.e. tags: ["tag1","tag2"]
- Convert Field
	- path
	- type (one of: long, double, string, boolean)
- Date
	- field 
	- targetField
	- formats - one of these: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
	- timeZone - one of these: https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html
- Drop
	- percentage, default to 100 which is full drop, can be used to throttle
- Geo IP
	- sourceField
	- targetField
	- properties
	- tagsOnSuccess
- Json
	- field
	- targetField
- Key Value
	- field
	- targetField
	- includeKeys
	- excludeKeys
	- trim
	- trimKey
- Remove Field
	- path (dotted path, i.e: a.b.c)
- Remove Tag
	- tags - list of tags
- Rename Field
	- from - the field name to rename
	- to - the new name of that field
- Substitue 
	- field
	- pattern
	- replacement
- User Agent
	- field
	- targetField
	- prefix
  
## Open source
In order to move to public repository, these are the items we need to fix:
- Remove the dependency of custom java-grok in Nexus
