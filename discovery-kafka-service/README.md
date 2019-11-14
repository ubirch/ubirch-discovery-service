# Kafka module for discovery service
A simple way to store a relathionship between two nodes in a JanusGraph server

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites and installation
Note: a local version of Kafka is not needed if the goal is to run the tests
* A local version of the [Kafka platform](https://kafka.apache.org/quickstart)
    * Start zookeeper and the server 

    ```bash 
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
    ```

    * Create a topic ```bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic TOPIC_NAME```

    * Start a producer ```bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TOPIC_NAME```

    * Start two consumers:
        * One watching the normal topic
        ```bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TOPIC_NAME --from-beginning```

        * One watching the error topic
        ```bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ERROR_TOPIC_NAME --from-beginning```

* A 0.3.x JanusGraph instance. 

    * The easiest way is to run the [standalone version](https://github.com/JanusGraph/janusgraph/releases/tag/v0.3.1) ```bin/janusgraph.sh start```
    * To reproduce the production environment, just docker-compose [this](https://github.com/ubirch/ubirch-discovery-service/tree/master/discovery-service-docker-jg).
    It'll start two docker containers: a cassandra and a JanusGraph instance

## Running the tests

**MAKE SURE TO *ABSOLUTELY NOT* RUN THE TESTS ON A PRODUCTION SERVER**. The database that JG will connect to will be periodically deleted through the various stages of the test

To run the tests, only a JanusGraph instance is needed.

Change the JanusGraph port and address in resources/application.base.conf with your JanusGraph's address and port (127.0.0.1 and 8182 by default)

Change the Kafka Configuration port and address in resources/application.base.conf to
```yaml
bootstrapServers = "localhost:9092"
topic = "com.ubirch.eventlog.discovery, test"
errorTopic = "test-error"
```

Run ```mvn test``` or use your IDE built-in test module to execute the tests

### Break down into end to end tests

All tests extends the [TestBase](https://github.com/ubirch/ubirch-discovery-service/blob/master/discovery-service-kafka/src/test/scala/com/ubirch/discovery/kafka/TestBase.scala)
trait that defines some necessary functions as well as which library should be included in the test module

All tests begin with the configuration of an embedded kafka server/client/producer

```
implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        kafkaPort = 9092,
        zooKeeperPort = PortGiver.giveMeZookeeperPort
      )
```

Some more configuration / preparation steps can be found before the line ```withRunningKafka```appears. This command
will start a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.

In this body, a producer will send requests to the discovery-service-kafka module, that will store it in the configured 
JanusGraph instance and verify that the data has been correctly added.

#### Invalid requests
To test invalid request, the procedure is the following:
* Delete the database
* Read the first line of all file in resource/invalid/requests/[storing or parsing]/
* For each file
    * A kafka consumer send the request to the test topic
    * Verify that the corresponding error available on resources/invalid/expectedResults/[storing or parsing]/ is 
    thrown by the producer
* Verify that no element were added to the database 

#### Valid requests
Testing of valid requests is done in a similar way, except that the expected results files consists of two comma 
separated values that represents the number of vertices and edges that should be present in the DB once the request is executed

The test of valid requests does **not** test if the properties and labels of each elements are correctly passed. Those 
kind of tests are done in the core library.

### Adding more tests
To add an (in)valid request to test, simply
* Add the one-line request under resources/(in)valid/requests/[storing or parsing]/
* Add the one-line expected response under resources/(in)valid/expectedResults/[storing or parsing]/

The DefaultStringConsumerSpec will automatically execute it next time

As all requests / expected response must fit on one line, comments can be added under those requests
# Deployment
TODO