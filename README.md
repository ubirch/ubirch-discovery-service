# ubirch-discovery-service
discovering ubirch made easy. Storing data on JanusGraph.

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing
purposes. See deployment for notes on how to deploy the project on a live system.

## Features
This project gives an easy way to store a relationship between two elements. 
## Prerequisites
* Maven
* JDK 1.8
* Scala 2.12.x

### Kafka
Make sure that the Service object is started and that you're using the topic configured in resources/application.conf

Use a kafka producer to send the queries

Requests should have the following format
```json
[{
	"v_from": {
		"properties": {
			"propertyName1": "propertyValue1",
			"propertyName2": "propertyValue2"
		},
		"label": "labelName"
	},
	"v_to": {
		"properties": {
			"name": "aName"
		},
		"label": "v2"
	},
	"edge": {
		"properties": {
			"hrue": "frehou"
		},
		"label": "v2"
	}
}]
```
Such request will create two vertex and link them together

## Quick gremlin console connection

```
gremlin> :rem connect tinkerpop.server conf/remote.yaml session
gremlin> :rem console
gremlin> graph = JanusGraphFactory.open('/etc/opt/janusgraph/janusgraph.properties')
gremlin> mgmt = graph.openManagement()
gremlin> mgmt
```
