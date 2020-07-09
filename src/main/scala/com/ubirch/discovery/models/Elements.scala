package com.ubirch.discovery.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.models.PropertyType.PropertyType
import com.ubirch.discovery.util.Util
import gremlin.scala.{ Key, KeyValue, Vertex }
import org.json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization

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

abstract class ElementCore(val properties: List[ElementProperty], val label: String) extends LazyLogging {

  def equals(that: ElementCore): Boolean

  def sortProperties: List[ElementProperty] = {
    properties.sortBy(x => x.keyName)
  }

  def toJson = {
    ("label" -> label) ~
      ("properties" -> properties.map { p => Util.kvToJson(p) })
  }

  override def toString: String = compact(render(toJson))

  def equalsUniqueProperty(that: ElementCore)(implicit propSet: Set[Property]): Boolean = {
    this.getUniqueProperties.exists(uniqueProp => that.getUniqueProperties.contains(uniqueProp))
  }

  def containsUniqueProperty(p: ElementProperty)(implicit propSet: Set[Property]): Boolean = {
    this.getUniqueProperties.exists(uniqueProp => uniqueProp.equals(p))
  }

  def getUniqueProperties(implicit propSet: Set[Property]): List[ElementProperty] = {
    properties filter (p => p.isUnique)
  }

  def mergeWith(that: ElementCore)(implicit propSet: Set[Property]): VertexCore = {
    if (!equalsUniqueProperty(that)) throw new Exception(s"can not merge this ${this.toString} with that ${that.toString}! not equal in term of unique props")
    if (that.label != label) {
      //TODO: dirty hack, fix
      if (this.label == "MASTER_TREE" || that.label == "MASTER_TREE") {
        logger.info(s"merged ${this.toString} with ${that.toString} even though they had different label")
        val mergedProps = this.properties.union(that.properties).distinct
        VertexCore(mergedProps, "MASTER_TREE")
      } else {
        throw new Exception(s"can not merge this ${this.toString} with that ${that.toString}! not equal in term of label")
      }
    } else {
      val mergedProps = this.properties.union(that.properties).distinct
      VertexCore(mergedProps, this.label)
    }

  }

}

case class VertexCore(override val properties: List[ElementProperty], override val label: String) extends ElementCore(properties, label) {

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }

  def addProperty(newProperty: ElementProperty): VertexCore = copy(properties = newProperty :: properties)

}

case class EdgeCore(override val properties: List[ElementProperty], override val label: String) extends ElementCore(properties, label) {

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }

  def addProperty(newProperty: ElementProperty): EdgeCore = copy(properties = newProperty :: properties)

}

case class DumbRelation(vFrom: Vertex, vTo: Vertex, edge: EdgeCore)

case class Relation(vFrom: VertexCore, vTo: VertexCore, edge: EdgeCore) {

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def toString: String = {
    compact(render(toJson))
  }

  def toJson: json4s.JObject = {
    ("v_from" -> vFrom.toJson) ~
      ("v_to" -> vTo.toJson) ~
      ("edge" -> edge.toJson)
  }
}

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

  def isUnique(implicit propSet: Set[Property]): Boolean = {
    propSet.exists(p => p.name.equalsIgnoreCase(this.keyName) && p.isPropertyUnique)
  }

  def equals(that: ElementProperty): Boolean = this.keyName.equals(that.keyName) && this.value.equals(that.value)
}
