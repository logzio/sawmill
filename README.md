![Sawmill Logo](logo.png)

[![Build Status](https://travis-ci.org/logzio/sawmill.svg?branch=master)](https://travis-ci.org/logzio/sawmill)
Sawmill is a JSON transformation open source library. 

It enables you to enrich, transform, and filter your JSON documents. 

Using Sawmill pipelines you can integrate your favorite groks, geoip, user-agent resolving, add or remove fields/tags and more in a descriptive manner, using configuration files or builders, in a simple DSL, allowing you to dynamically change transformations.

# Documentation
The full Sawmill documentation [can be found here](https://github.com/logzio/sawmill/wiki).

## Simple configuration example
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
