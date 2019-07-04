package com.ubirch.discovery.core.connector

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.kafka.express.ConfigBase
import gremlin.scala._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph

import scala.concurrent.Future

/**
  * Factory that allows a single instance of the graph to be available through the entire program
  */
object GremlinConnector {
  private val instance = new GremlinConnector
  def get: GremlinConnector = instance

  /*
  Loads the properties contained in ressources/application.conf in the cluster
   */
  def buildProperties(config: Config) = {
    val conf = new PropertiesConfiguration()
    conf.addProperty("hosts", config.getString("core.connector.hosts"))
    conf.addProperty("port", config.getString("core.connector.port"))
    conf.addProperty("serializer.className", config.getString("core.connector.serializer.className"))
    conf.addProperty("connectionPool.maxWaitForConnection", config.getString("core.connector.connectionPool.maxWaitForConnection"))
    conf.addProperty("connectionPool.reconnectInterval", config.getString("core.connector.connectionPool.reconnectInterval"))
    // no idea why the following line needs to be duplicated. Doesn't work without
    // cf https://stackoverflow.com/questions/45673861/how-can-i-remotely-connect-to-a-janusgraph-server first answer, second comment ¯\_ツ_/¯
    conf.addProperty("serializer.config.ioRegistries", config.getAnyRef("core.connector.serializer.config.ioRegistries").asInstanceOf[java.util.ArrayList[String]])
    conf.addProperty("serializer.config.ioRegistries", config.getStringList("core.connector.serializer.config.ioRegistries"))
    conf
  }
}

/**
  * Class allowing the connection to the graph contained in the JanusGraph server
  * graph: the graph
  * g: the traversal of the graph
  * cluster: the cluster used by the graph to connect to the janusgraph server
  */
class GremlinConnector private () extends LazyLogging with ConfigBase {

  val cluster: Cluster = Cluster.open(GremlinConnector.buildProperties(conf))

  implicit val graph: ScalaGraph = EmptyGraph.instance.asScala.configure(_.withRemote(DriverRemoteConnection.using(cluster)))
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance

  def closeConnection(): Unit = {
    cluster.close()
  }

  Lifecycle.get.addStopHook { () =>
    logger.info("Shutting down connection with Janus: " + cluster.toString)
    Future.successful(closeConnection())
  }

}
