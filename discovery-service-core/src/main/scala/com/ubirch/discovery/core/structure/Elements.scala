package com.ubirch.discovery.core.structure

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import gremlin.scala.{KeyValue, TraversalSource}

object Elements {

  abstract class Types(val name: String) {
    val elementType: ElementType

    override def toString: String = name
  }

  abstract class ElementType(val name: String) {
    override def toString: String = name
  }

  class Property(name: String, isUnique: Boolean = false) extends Types(name) {
    override val elementType: ElementType = TheProperty

    def isPropertyUnique: Boolean = isUnique
  }

  abstract class Label(name: String) extends Types(name) {
    val elementType: ElementType = TheLabel
  }

  case object TheProperty extends ElementType("Property")

  case object TheLabel extends ElementType("Label")

}

abstract class ElementCore(properties: List[KeyValue[String]], label: String) {

  def equals(that: ElementCore): Boolean

  def sortProperties: List[KeyValue[String]] = {
    properties.sortBy(x => x.key.name)
  }
}

case class VertexCore(properties: List[KeyValue[String]], label: String) extends ElementCore(properties, label) {
  def toVertexStructDb(g: TraversalSource)(implicit propSet: Set[Property]): VertexServer = {
    new VertexServer(this, g)
  }

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }
}

case class EdgeCore(properties: List[KeyValue[String]], label: String) extends ElementCore(properties, label) {

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }
}

case class Relation(vFrom: VertexCore, vTo: VertexCore, edge: EdgeCore) {
  def toRelationServer(implicit propSet: Set[Property], gc: GremlinConnector): RelationServer = {
    val vFrom: VertexServer = this.vFrom.toVertexStructDb(gc.g)
    val vTo: VertexServer = this.vTo.toVertexStructDb(gc.g)
    RelationServer(vFrom, vTo, edge)
  }
}

case class RelationServer(vFromDb: VertexServer, vToDb: VertexServer, edge: EdgeCore)
