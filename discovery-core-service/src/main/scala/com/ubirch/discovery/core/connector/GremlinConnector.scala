package com.ubirch.discovery.core.connector

import gremlin.scala.{ ScalaGraph, TraversalSource }
import org.apache.tinkerpop.gremlin.process.traversal.Bindings

trait GremlinConnector {
  def graph: ScalaGraph
  def g: TraversalSource
  def b: Bindings
  def closeConnection()
}

