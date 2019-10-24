package com.ubirch.discovery.core.structure

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Util
import gremlin.scala.KeyValue
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.JsonDSL._

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

  def toJson = {
    ("label" -> label) ~
      ("properties" -> properties.map { p => Util.kvToJson(p) })

  }
}

case class VertexCore(properties: List[KeyValue[String]], label: String) extends ElementCore(properties, label) {
  def toVertexStructDb(gc: GremlinConnector)(implicit propSet: Set[Property]): VertexDatabase = {
    new VertexDatabase(this, gc)
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
    val vFrom: VertexDatabase = this.vFrom.toVertexStructDb(gc)
    val vTo: VertexDatabase = this.vTo.toVertexStructDb(gc)
    RelationServer(vFrom, vTo, edge)
  }

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def toString: String = {
    compact(render(toJson))
  }

  def toJson: json4s.JObject = {
    ("vFrom" -> vFrom.toJson) ~
      ("vTo" -> vTo.toJson) ~
      ("edge" -> edge.toJson)
  }
}

case class RelationServer(vFromDb: VertexDatabase, vToDb: VertexDatabase, edge: EdgeCore)
