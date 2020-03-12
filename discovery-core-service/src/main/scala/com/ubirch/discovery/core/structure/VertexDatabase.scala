package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import com.ubirch.discovery.core.util.Timer
import gremlin.scala.{KeyValue, TraversalSource, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.Bindings

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
    timedPossibleVertex.logTimeTaken(s"check if vertex ${coreVertex.toString} was already in the DB. result: ${timedPossibleVertex.result.get}")
    timedPossibleVertex.result.get
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
      for (property <- coreVertex.properties.tail) {
        logger.debug(s"on vertex with id: $vertexId adding property [${property.keyName} : ${property.value}]")
        addPropertyToVertex(property.toKeyValue)
      }
    } catch {
      case e: CompletionException =>
        logger.error(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage)
        throw new ImportToGremlinException(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage) //TODO: do something
      case e: Exception => throw e
    }
  }

  private def initialiseVertex: Vertex = {
    g.addV(b.of("label", coreVertex.label)).property(coreVertex.properties.head.toKeyValue).l().head
  }

  private def addPropertyToVertex[T](property: KeyValue[T], vertex: Vertex = vertex) = {
    g.V(vertex).property(property).iterate()
  }

  /**
    * Add new properties to vertex
    */
  def update() = {
    addNewPropertiesToVertex(vertex)
  }

  private def addNewPropertiesToVertex(vertex: Vertex): Unit = {
    Timer.time({
      for (property <- coreVertex.properties) {
        if (!doesPropExist(property.toKeyValue)) {
          addPropertyToVertex(property.toKeyValue, vertex: Vertex)
          logger.debug(s"Adding property: ${property.keyName}")
        }
      }
    }).logTimeTaken(s"add properties to vertex with id: ${vertex.id().toString}")

    def doesPropExist[T](keyV: KeyValue[T]): Boolean = g.V(vertex).properties(keyV.key.name).toList().nonEmpty
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

