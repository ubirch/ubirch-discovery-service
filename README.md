# ubirch-discovery-service
discovering ubirch made easy. Storing data on JanusGraph and visualising them.

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing
purposes. See deployment for notes on how to deploy the project on a live system.

## Features
This project gives an easy way to store and access a relationship between two elements. This project is composed
of several modules:
* Core: base library that allows a bridge between other endpoints and JanusGraph
* JanusGraph: properties for the official JanusGraph docker-image
* Kafka: a kafka endpoint that allows the storing of data
* Rest: a REST endpoint with Swagger support that allows the storing and queering of graphs

## Prerequisites
* Maven
* JDK 1.8
* Scala 2.12.x

## Code example
### Core
#### Add two Vertices and link them with
```scala
// First, create a gremlinConnector 
import com.ubirch.discovery.services.connector.GremlinConnector
implicit val gc: GremlinConnector = GremlinConnector.get

// Use the AddVertices class to add two vertex
import com.ubirch.discovery.core.operation.AddVertices
AddVertices().addTwoVertices(idFristVertex, PropertiesFirstVertex, LabelFirstVertex)
  (idSecondVertex, PropertiesSecondVertex, LabelSecondVertex)
  (edgeProperties, edgeLabel)
```

#### Get all the neighbors within a depth of 3 from a specific vertex
```scala
import com.ubirch.discovery.core.operation.GetVertices
GetVertices().getVertexDepth(idVertex, 3)
```

### Kafka
Make sure that the Service object is started and that you're using the topic configured in resources/application.conf

Use a kafka producer to send the queries

Requests should have the following format
```json
[{
	"v1": {
		"id": "1",
		"properties": {
			"propertyName1": "propertyValue1",
			"propertyName2": "propertyValue2"
		},
		"label": "labelName"
	},
	"v2": {
		"id": "2",
		"properties": {
			"name": "aName"
		},
		"label": "v2"
	},
	"edge": {
		"properties": {
			"hrue": "frehou"
		}
	}
}]
```
Such request will create two vertex and link them together

### REST
TODO.

### JanusGraph
