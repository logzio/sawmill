![Sawmill Logo](sawmill-logo.png)

[![Build Status](https://github.com/logzio/sawmill/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/logzio/sawmill/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.logz.sawmill/sawmill/badge.svg)](http://mvnrepository.com/artifact/io.logz.sawmill/sawmill)

*Update: June 25, 2020*
The 2.0 release of Sawmill introduces a breaking change to the *GeoIpProcessor* to comply with the updated license of the MaxMind Lite database.
See https://github.com/logzio/sawmill/wiki/GeoIp-Processor for additional details.

Sawmill is a JSON transformation open source library. 

It enables you to enrich, transform, and filter your JSON documents. 

Using Sawmill pipelines you can integrate your favorite groks, geoip, user-agent resolving, add or remove fields/tags and more in a descriptive manner, using configuration files or builders, in a simple DSL, allowing you to dynamically change transformations.

## Download

Get Sawmill Java via Maven:

```xml
<dependency>
    <groupId>io.logz.sawmill</groupId>
    <artifactId>sawmill-core</artifactId>
    <version>2.0.21</version>
</dependency>
```

or Gradle:

```gradle
compile 'io.logz.sawmill:sawmill-core:2.0.21'
```

## Documentation

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
