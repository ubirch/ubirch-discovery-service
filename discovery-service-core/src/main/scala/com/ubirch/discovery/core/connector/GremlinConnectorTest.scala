package com.ubirch.discovery.core.connector

import gremlin.scala._
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

protected class GremlinConnectorTest extends GremlinConnector {

  implicit val graph: ScalaGraph = TinkerGraph.open().asScala
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance

  override def closeConnection(): Unit = graph.close()

}
