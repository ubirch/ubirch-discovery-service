package com.ubirch.discovery.core.structure

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure.PropertyType.PropertyType
import com.ubirch.discovery.core.util.Util
import gremlin.scala.{Key, KeyValue}
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

abstract class ElementCore(properties: List[ElementProperty], label: String) {

  def equals(that: ElementCore): Boolean

  def sortProperties: List[ElementProperty] = {
    properties.sortBy(x => x.keyName)
  }

  def toJson = {
    ("label" -> label) ~
      ("properties" -> properties.map { p => Util.kvToJson(p) })

  }
}

case class VertexCore(properties: List[ElementProperty], label: String) extends ElementCore(properties, label) {
  def toVertexStructDb(gc: GremlinConnector)(implicit propSet: Set[Property]): VertexDatabase = {
    new VertexDatabase(this, gc)
  }

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }
}

case class EdgeCore(properties: List[ElementProperty], label: String) extends ElementCore(properties, label) {

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

object PropertyType extends Enumeration {
  type PropertyType = Value
  val String, Long = Value
}

case class ElementProperty(keyValue: KeyValue[Any], propType: PropertyType) {
  def toKeyValue: KeyValue[_ >: String with Long] = {
    propType match {
      case PropertyType.String => KeyValue[String](Key(keyValue.key.name), keyValue.value.asInstanceOf[String])
      case PropertyType.Long => KeyValue[Long](Key(keyValue.key.name), keyValue.value.asInstanceOf[Long])
    }
  }

  def keyName: String = keyValue.key.name

  def value = {
    propType match {
      case PropertyType.String => keyValue.value.asInstanceOf[String]
      case PropertyType.Long => keyValue.value.asInstanceOf[Long]
    }
  }
}
