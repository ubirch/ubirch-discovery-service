package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure.PropertyType.PropertyType
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import com.ubirch.discovery.core.util.Util
import gremlin.scala.{ Edge, Key, KeyValue, Vertex }
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

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

  override def toString: String = compact(render(toJson))

  def equalsUniqueProperty(that: ElementCore)(implicit propSet: Set[Property]): Boolean = {
    this.getUniqueProperties.exists(uniqueProp => that.getUniqueProperties.contains(uniqueProp))
  }

  def getUniqueProperties(implicit propSet: Set[Property]): List[ElementProperty] = {
    properties filter (p => p.isUnique)
  }

}

case class VertexCore(properties: List[ElementProperty], label: String)(implicit ec: ExecutionContext) extends ElementCore(properties, label) {
  def toVertexStructDb(gc: GremlinConnector)(implicit propSet: Set[Property]): VertexDatabase = {
    new VertexDatabase(this, gc)
  }

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }

  def addProperty(newProperty: ElementProperty): VertexCore = copy(properties = newProperty :: properties)

}

case class EdgeCore(properties: List[ElementProperty], label: String) extends ElementCore(properties, label) {

  def equals(that: ElementCore): Boolean = {
    this.sortProperties equals that.sortProperties
  }

  def addProperty(newProperty: ElementProperty): EdgeCore = copy(properties = newProperty :: properties)

}

case class Relation(vFrom: VertexCore, vTo: VertexCore, edge: EdgeCore)(implicit ec: ExecutionContext) {
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
    ("v_from" -> vFrom.toJson) ~
      ("v_to" -> vTo.toJson) ~
      ("edge" -> edge.toJson)
  }
}

case class RelationServer(vFromDb: VertexDatabase, vToDb: VertexDatabase, edge: EdgeCore)(implicit ec: ExecutionContext) extends LazyLogging {

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def toString: String = {
    compact(render(toJson))
  }

  def toJson = {
    ("v_from" -> vFromDb.coreVertex.toJson) ~
      ("v_to" -> vToDb.coreVertex.toJson) ~
      ("edge" -> edge.toJson)
  }

  private def createEdgeTraversalPromise(vFrom: Vertex, vTo: Vertex)(implicit gc: GremlinConnector): Future[List[Edge]] = {
    var constructor = gc.g.V(vTo).addE(edge.label)
    for (prop <- edge.properties) {
      constructor = constructor.property(prop.toKeyValue)
    }
    constructor.from(vFrom).promise()
  }

  def createEdge(implicit gc: GremlinConnector): Future[Option[Edge]] = {

    val vertices: Future[(Vertex, Vertex)] = for {
      maybeVFrom <- vFromDb.vertex
      maybeVTo <- vToDb.vertex
    } yield {

      val vFrom = maybeVFrom match {
        case Some(v) => v
        case None => throw new ImportToGremlinException(s"can not find vertex ${vFromDb.toString} in graph, while it should have already been created")
      }

      val vTo = maybeVTo match {
        case Some(v) => v
        case None => throw new ImportToGremlinException(s"can not find vertex ${vToDb.toString} in graph, while it should have already been created")
      }

      (vFrom, vTo)
    }

    if (edge.properties.isEmpty) {
      for {
        (vFrom, vTo) <- vertices
        createdEdge: immutable.Seq[Edge] <- gc.g.V(vFrom).as("a").V(vTo).addE(edge.label).from(vFrom).promise()
      } yield {
        createdEdge.headOption
      }
    } else {
      for {
        (vFrom, vTo) <- vertices
        createdEdge <- createEdgeTraversalPromise(vFrom, vTo)
      } yield {
        createdEdge.headOption
      }
    }

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
