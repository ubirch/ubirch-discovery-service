package com.ubirch.discovery.services.connector

import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.Lifecycle
import com.ubirch.discovery.services.connector.App.buildProperties
import gremlin.scala._
import org.apache.commons.configuration2.PropertiesConfiguration

import javax.inject.{ Inject, Singleton }
import org.apache.tinkerpop.gremlin.driver.{ Client, Cluster }
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry

import java.util
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

/**
  * Class allowing the connection to the graph contained in the JanusGraph server
  * graph: the graph
  * g: the traversal of the graph
  * cluster: the cluster used by the graph to connect to the janusgraph server
  */
@Singleton
class JanusGraphConnector @Inject() (lifecycle: Lifecycle, config: Config) extends GremlinConnector with LazyLogging {

  val cluster: Cluster = buildCluster(config)

  implicit val graph: ScalaGraph = EmptyGraph.instance.asScala.configure(_.withRemote(DriverRemoteConnection.using(cluster)))

  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance // see https://groups.google.com/forum/#!topic/janusgraph-users/T7wg_dKri1g for binding usages (tl;dr: only use them in lambdas functions)

  def closeConnection(): Unit = {
    cluster.close()
  }

  def buildCluster(config: Config): Cluster = {
    val cluster = Cluster.build()
    val hosts: List[String] = config.getString("core.connector.hosts")
      .split(",")
      .toList
      .map(_.trim)
      .filter(_.nonEmpty)

    cluster.addContactPoints(hosts: _*)
      .port(config.getInt("core.connector.port"))

    val maxWaitForConnection = config.getInt("core.connector.connectionPool.maxWaitForConnection")
    if (maxWaitForConnection > 0) cluster.maxWaitForConnection(maxWaitForConnection)
    Try(config.getInt("settings.connectionPool.maxContentLength")) match {
      case Success(value) => cluster.maxContentLength(value)
      case Failure(_) => cluster.maxContentLength(4096000)
    }

    val reconnectInterval = config.getInt("core.connector.connectionPool.reconnectInterval")
    if (reconnectInterval > 0) cluster.reconnectInterval(reconnectInterval)

    val connectionMinSize = config.getInt("core.connector.connectionPool.minSize")
    if (connectionMinSize > 0) cluster.minConnectionPoolSize(connectionMinSize)

    val connectionMaxSize = config.getInt("core.connector.connectionPool.maxSize")
    if (connectionMaxSize > 0) cluster.maxConnectionPoolSize(connectionMaxSize)

    val nioPoolSize = config.getInt("core.connector.nioPoolSize")
    if (nioPoolSize > 0) cluster.nioPoolSize(nioPoolSize)

    val workerPoolSize = config.getInt("core.connector.workerPoolSize")
    if (workerPoolSize > 0) cluster.workerPoolSize(workerPoolSize)

    val conf = new util.HashMap[String, AnyRef]()
    conf.put("serializer.config.ioRegistries", config.getAnyRef("core.connector.serializer.config.ioRegistries").asInstanceOf[java.util.ArrayList[String]])
    val serializer = new GraphBinaryMessageSerializerV1()
    serializer.configure(conf, null)

    cluster.serializer(serializer).create()
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down connection with Janus: " + cluster.toString)
    Future.successful(closeConnection())
  }

}
