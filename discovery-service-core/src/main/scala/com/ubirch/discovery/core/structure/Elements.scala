package com.ubirch.discovery.core.structure

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

abstract class ElementToAdd(properties: List[KeyValue[String]], label: String) {

  def equals(that: ElementToAdd): Boolean

  def sortProperties: List[KeyValue[String]] = {
    properties.sortBy(x => x.key.name)
  }
}

case class VertexToAdd(properties: List[KeyValue[String]], label: String) extends ElementToAdd(properties, label) {
  def toVertexStructDb(g: TraversalSource)(implicit propSet: Set[Property]): VertexStructDb = {
    new VertexStructDb(this, g)
  }

  def equals(that: ElementToAdd): Boolean = {
    this.sortProperties equals that.sortProperties
  }
}

case class EdgeToAdd(properties: List[KeyValue[String]], label: String) extends ElementToAdd(properties, label) {

  def equals(that: ElementToAdd): Boolean = {
    this.sortProperties equals that.sortProperties
  }
}

case class Relation(vFrom: VertexToAdd, vTo: VertexToAdd, edge: EdgeToAdd)

case class RelationDb(vFromDb: VertexStructDb, vToDb: VertexStructDb, edge: EdgeToAdd)
