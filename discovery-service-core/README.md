# Core module of the discovery service
A simple way to store a relathionship between two nodes in a JanusGraph server

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites and installation
* A 0.3.x JanusGraph instance. 

    * The easiest way is to run the [standalone version](https://github.com/JanusGraph/janusgraph/releases/tag/v0.3.1) ```bin/janusgraph.sh start```
    * To reproduce the production environment, just docker-compose [this](https://github.com/ubirch/ubirch-discovery-service/tree/master/discovery-service-docker-jg).
    It'll start two docker containers: a cassandra and a JanusGraph instance
    
## Running the tests

**MAKE SURE TO *ABSOLUTELY NOT* RUN THE TESTS ON A PRODUCTION SERVER**. 
The database that JG will connect to will be periodically deleted through the various stages of the test

To run the tests, only a JanusGraph instance is needed.

Change the JanusGraph port and address in resources/application.base.conf with your JanusGraph's address 
and port (127.0.0.1 and 8182 by default)

Run ```mvn test``` or use your IDE built-in test module to execute the tests

### Break down into end to end tests


Some more configuration / preparation steps can be found before the line ```withRunningKafka```appears. This command
will start a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.

In this body, a producer will send requests to the discovery-service-kafka module, that will store it in the configured 
JanusGraph instance and verify that the data has been correctly added.

