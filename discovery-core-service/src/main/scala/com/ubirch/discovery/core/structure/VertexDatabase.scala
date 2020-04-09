package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import com.ubirch.discovery.core.util.Timer
import gremlin.scala.{TraversalSource, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.json4s.JsonDSL._

import scala.collection.JavaConverters._

class VertexDatabase(val coreVertex: VertexCore, val gc: GremlinConnector)(implicit propSet: Set[Property]) extends LazyLogging {

  override def toString: String = coreVertex.toString

  val g: TraversalSource = gc.g
  val b: Bindings = gc.b

  var vertex: gremlin.scala.Vertex = { // if error check that gremlin.scala.Vertex is the correct type that should be returned
    def searchForVertexByProperties(properties: List[ElementProperty]): gremlin.scala.Vertex = {
      properties match {
        case Nil => null
        case property :: restOfProperties =>
          if (!isPropertyIterable(property.keyName)) searchForVertexByProperties(restOfProperties) else
            g.V().has(property.toKeyValue).headOption() match {
              case Some(v) => v
              case None => searchForVertexByProperties(restOfProperties)
            }
      }
    }
    val timedPossibleVertex = Timer.time({
      val possibleVertex = searchForVertexByProperties(coreVertex.properties)
      if (possibleVertex != null) {
        addNewPropertiesToVertex(possibleVertex)
      }
      possibleVertex
    })
    timedPossibleVertex.logTimeTakenJson("check_vertex_in_db" -> List(("result" -> Option(timedPossibleVertex.result.getOrElse("false")).getOrElse("false").toString) ~ ("vertex" -> coreVertex.toJson)))
    timedPossibleVertex.result.getOrElse(null)
  }

  def isPropertyIterable(propertyName: String): Boolean = {

    def checkOnProps(set: Set[Property]): Boolean = {
      set.toList match {
        case Nil => false
        case x => if (x.head.name == propertyName) {
          if (x.head.isPropertyUnique) true else checkOnProps(x.tail.toSet)
        } else {
          checkOnProps(x.tail.toSet)
        }
      }
    }
    checkOnProps(propSet)

  }

  def existInJanusGraph: Boolean = vertex != null

  /**
    * Adds a vertex in the database with his label and properties.
    */
  def addVertexWithProperties(): Unit = {
    if (existInJanusGraph) throw new ImportToGremlinException("Vertex already exist in the database")
    try {
      logger.debug(s"adding vertex ${coreVertex.toString}")
      vertex = initialiseVertex
    } catch {
      case e: CompletionException =>
        logger.error(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage)
        throw new ImportToGremlinException(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage) //TODO: do something
      case e: Exception => throw e
    }
  }

  private def initialiseVertex: Vertex = {
    var constructor = gc.g.addV(coreVertex.label)
    for (prop <- coreVertex.properties) {
      constructor = constructor.property(prop.toKeyValue)
    }
    constructor.l().head
  }

  /**
    * Add new properties to vertex
    */
  def update(): Unit = {
    addNewPropertiesToVertex(vertex)
  }

  private def addNewPropertiesToVertex(vertex: Vertex): Unit = {
    val r = Timer.time({
      var constructor = gc.g.V(vertex)
      for (prop <- coreVertex.properties) {
        constructor = constructor.property(prop.toKeyValue)
      }
      constructor.l().head
    })
    r.logTimeTaken(s"add properties to vertex with id: ${vertex.id().toString}")
    if (r.result.isFailure) throw new Exception(s"error adding properties on vertex ${vertex.toString}", r.result.failed.get)
  }

  /**
    * Returns a Map<Any, List<Any>> fo the properties. A vertex property can have a list of values, thus why
    * the method is returning this kind of structure.
    *
    * @return A map containing the properties name and respective values of the vertex contained in this structure.
    */
  def getPropertiesMap: Map[Any, List[Any]] = {
    val propertyMapAsJava = g.V(vertex).valueMap.toList().head.asScala.toMap.asInstanceOf[Map[Any, util.ArrayList[Any]]]
    propertyMapAsJava map { x => x._1 -> x._2.asScala.toList }
  }

  def deleteVertex(): Unit = {
    if (existInJanusGraph) g.V(vertex.id).drop().iterate()
  }

  def vertexId: String = vertex.id().toString

}

