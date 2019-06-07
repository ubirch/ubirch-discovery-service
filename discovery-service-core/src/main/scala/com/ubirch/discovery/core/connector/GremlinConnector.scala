package com.ubirch.discovery.core.connector

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import gremlin.scala._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph

import scala.concurrent.Future

object GremlinConnector {
  private val instance = new GremlinConnector
  def get: GremlinConnector = instance
}

class GremlinConnector() extends LazyLogging {

  // cluster config
  val  conf = new PropertiesConfiguration()

  val config: Config = ConfigFactory.load("cluster-docker.conf")

  logger.info(config.getString("core.connector.hosts"))

  conf.addProperty("hosts", config.getString("core.connector.hosts"))
  conf.addProperty("port", config.getString("core.connector.port"))
  conf.addProperty("serializer.className", config.getString("core.connector.serializer.className"))
  // no idea why the following line needs to be duplicated. Doesn't work without, cf https://stackoverflow.com/questions/45673861/how-can-i-remotely-connect-to-a-janusgraph-server first answer, second comment ¯\_ツ_/¯
  conf.addProperty("serializer.config.ioRegistries", config.getAnyRef("core.connector.serializer.config.ioRegistries").asInstanceOf[java.util.ArrayList[String]])
  conf.addProperty("serializer.config.ioRegistries", config.getStringList("core.connector.serializer.config.ioRegistries"))

  val cluster: Cluster = Cluster.open(conf)

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
