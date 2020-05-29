package com.ubirch.discovery.core.connector

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.discovery.core.util.Util
import com.ubirch.kafka.express.ConfigBase
import gremlin.scala._
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.janusgraph.core.{ JanusGraph, JanusGraphFactory }

import scala.concurrent.Future

/**
  * Class allowing the connection to the graph contained in the JanusGraph server
  * graph: the graph
  * g: the traversal of the graph
  * cluster: the cluster used by the graph to connect to the janusgraph server
  */
protected class JanusGraphConnector extends GremlinConnector with LazyLogging with ConfigBase {

  val janusgraphProperties = conf.getString("janus.properties")

  //val cluster: Cluster = Cluster.open(GremlinConnectorFactory.buildProperties(conf))

  implicit val graph: ScalaGraph = JanusGraphFactory.open(janusgraphProperties).asScala()

  //EmptyGraph.instance.asScala.configure(_.withRemote(DriverRemoteConnection.using(cluster)))
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance // see https://groups.google.com/forum/#!topic/janusgraph-users/T7wg_dKri1g for binding usages (tl;dr: only use them in lambdas functions)

  def closeConnection(): Unit = {
    graph.close()
  }

  Lifecycle.get.addStopHook { () =>
    logger.info("Shutting down janusgraph server ")
    Future.successful(closeConnection())
  }

}
