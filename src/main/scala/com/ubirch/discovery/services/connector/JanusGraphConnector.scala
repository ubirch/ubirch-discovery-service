package com.ubirch.discovery.services.connector

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.Lifecycle
import gremlin.scala._
import javax.inject.{ Inject, Singleton }
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

import scala.collection.JavaConverters._

/**
  * Class allowing the connection to the graph contained in the JanusGraph server
  * graph: the graph
  * g: the traversal of the graph
  * cluster: the cluster used by the graph to connect to the janusgraph server
  */
@Singleton
class JanusGraphConnector @Inject() (lifecycle: Lifecycle, config: Config) extends GremlinConnector with LazyLogging {

  val cluster: Cluster = Cluster.open(buildProperties(config))

  implicit val graph: ScalaGraph = EmptyGraph.instance.asScala.configure(_.withRemote(DriverRemoteConnection.using(cluster)))
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance // see https://groups.google.com/forum/#!topic/janusgraph-users/T7wg_dKri1g for binding usages (tl;dr: only use them in lambdas functions)

  def closeConnection(): Unit = {
    cluster.close()
  }

  def buildProperties(config: Config): PropertiesConfiguration = {
    val conf = new PropertiesConfiguration()

    val hosts: List[String] = config.getString("core.connector.hosts")
      .split(",")
      .toList
      .map(_.trim)
      .filter(_.nonEmpty)

    conf.addProperty("hosts", hosts.asJava)
    conf.addProperty("port", config.getString("core.connector.port"))
    conf.addProperty("serializer.className", config.getString("core.connector.serializer.className"))
    // no idea why the following line needs to be duplicated. Doesn't work without
    // cf https://stackoverflow.com/questions/45673861/how-can-i-remotely-connect-to-a-janusgraph-server first answer, second comment ¯\_ツ_/¯
    conf.addProperty("serializer.config.ioRegistries", config.getAnyRef("core.connector.serializer.config.ioRegistries").asInstanceOf[java.util.ArrayList[String]])
    conf.addProperty("serializer.config.ioRegistries", config.getStringList("core.connector.serializer.config.ioRegistries"))

    Try(config.getInt("settings.connectionPool.maxContentLength")) match {
      case Success(value) => conf.addProperty("settings.connectionPool.maxContentLength", value)
      case Failure(_) => conf.addProperty("settings.connectionPool.maxContentLength", 4096000)
    }

    val maxWaitForConnection = config.getInt("core.connector.connectionPool.maxWaitForConnection")
    if (maxWaitForConnection > 0) conf.addProperty("connectionPool.maxWaitForConnection", maxWaitForConnection)

    val reconnectInterval = config.getInt("core.connector.connectionPool.reconnectInterval")
    if (reconnectInterval > 0) conf.addProperty("connectionPool.reconnectInterval", reconnectInterval)

    val connectionMinSize = config.getInt("core.connector.connectionPool.minSize")
    if (connectionMinSize > 0) conf.addProperty("connectionPool.minSize", connectionMinSize)

    val connectionMaxSize = config.getInt("core.connector.connectionPool.maxSize")
    if (connectionMaxSize > 0) conf.addProperty("connectionPool.maxSize", connectionMaxSize)

    val nioPoolSize = config.getInt("core.connector.nioPoolSize")
    if (nioPoolSize > 0) conf.addProperty("nioPoolSize", nioPoolSize)

    val workerPoolSize = config.getInt("core.connector.workerPoolSize")
    if (workerPoolSize > 0) conf.addProperty("workerPoolSize", workerPoolSize)

    conf
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down connection with Janus: " + cluster.toString)
    Future.successful(closeConnection())
  }

}
