package com.ubirch.discovery.services.connector

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.Lifecycle
import com.ubirch.kafka.express.ConfigBase
import gremlin.scala._
import javax.inject.{ Inject, Singleton }
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph

import scala.concurrent.Future

/**
  * Class allowing the connection to the graph contained in the JanusGraph server
  * graph: the graph
  * g: the traversal of the graph
  * cluster: the cluster used by the graph to connect to the janusgraph server
  */
@Singleton
class JanusGraphConnector @Inject() (lifecycle: Lifecycle, config: Config) extends GremlinConnector with LazyLogging {

  val cluster: Cluster = Cluster.open(GremlinConnectorFactory.buildProperties(config))

  implicit val graph: ScalaGraph = EmptyGraph.instance.asScala.configure(_.withRemote(DriverRemoteConnection.using(cluster)))
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance // see https://groups.google.com/forum/#!topic/janusgraph-users/T7wg_dKri1g for binding usages (tl;dr: only use them in lambdas functions)

  def closeConnection(): Unit = {
    cluster.close()
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down connection with Janus: " + cluster.toString)
    Future.successful(closeConnection())
  }

}
