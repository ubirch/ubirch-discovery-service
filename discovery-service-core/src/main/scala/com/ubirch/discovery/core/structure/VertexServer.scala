package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import com.ubirch.discovery.core.util.Timer
import gremlin.scala.{KeyValue, TraversalSource, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.Bindings

import scala.collection.JavaConverters._

class VertexServer(val internalVertex: VertexCore, val g: TraversalSource)(implicit propSet: Set[Property]) extends LazyLogging {

  var vertex: gremlin.scala.Vertex = { // if error check that gremlin.scala.Vertex is the correct type that should be returned
    def searchForVertexByProperties(properties: List[KeyValue[String]]): gremlin.scala.Vertex = {
      properties match {
        case Nil => null
        case property :: restOfProperties =>
          if (!isPropertyIterable(property.key.name)) searchForVertexByProperties(restOfProperties) else
            g.V().has(property).headOption() match {
              case Some(v) => v
              case None => searchForVertexByProperties(restOfProperties)
            }
      }
    }
    val timer = new Timer()
    val possibleVertex = searchForVertexByProperties(internalVertex.properties)
    if (possibleVertex != null) {
      addNewPropertiesToVertex(possibleVertex)
    }
    timer.finish(s"check if vertex with properties ${internalVertex.properties.mkString(", ")} was already in the DB")
    possibleVertex
  }

  def isPropertyIterable(prop: String): Boolean = {

    def checkOnProps(set: Set[Property]): Boolean = {
      set.toList match {
        case Nil => false
        case x => if (x.head.name == prop) {
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
    *
    * @param b          Bindings for indexing.
    */
  def addVertexWithProperties(b: Bindings): Unit = {
    if (existInJanusGraph) throw new ImportToGremlinException("Vertex already exist in the database")
    try {
      logger.debug(s"adding vertex: label: ${internalVertex.label}; properties: ${internalVertex.properties.mkString(", ")}")
      vertex = initialiseVertex(b)
      for (property <- internalVertex.properties.tail) {
        logger.info(s"adding property ${property.key.name} , ${property.value}")
        logger.info("vertexId: " + vertexId)
        addPropertyToVertex(property)
      }
    } catch {
      case e: CompletionException => throw new ImportToGremlinException(e.getMessage) //TODO: do something
    }
  }

  private def initialiseVertex(b: Bindings): Vertex = {
    g.addV(b.of("label", internalVertex.label)).property(internalVertex.properties.head).l().head
  }

  private def addPropertyToVertex(property: KeyValue[String], vertex: Vertex = vertex) = {
    g.V(vertex).property(property).iterate()
  }

  private def addNewPropertiesToVertex(vertex: Vertex): Unit = {
    val timer = new Timer()
    for (property <- internalVertex.properties) {
      if (!doesPropExist(property)) {
        addPropertyToVertex(property, vertex: Vertex)
        logger.debug(s"Adding property: ${property.key.name}")
      }
    }
    timer.finish(s"add properties to vertex with id: ${vertex.id().toString}")

    def doesPropExist(keyV: KeyValue[String]): Boolean = g.V(vertex).properties(keyV.key.name).toList().nonEmpty
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

