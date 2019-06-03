package com.ubirch.discovery.core.connector

import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import gremlin.scala._
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

  val confPath: URL = getClass.getResource("/remote-objects.yaml")

  val cluster: Cluster = Cluster.open(confPath.getPath)

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
