package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import gremlin.scala.{KeyValue, TraversalSource}
import org.apache.tinkerpop.gremlin.process.traversal.Bindings

import scala.collection.JavaConverters._

class VertexStructDb(val properties: List[KeyValue[String]], val g: TraversalSource, label: String)(implicit propSet: Set[Property]) extends LazyLogging {

  var vertex: gremlin.scala.Vertex = { // if error check that gremlin.scala.Vertex is the correct type that should be returned
    def lookupByProps(propList: List[KeyValue[String]]): gremlin.scala.Vertex = {
      propList match {
        case Nil => null
        case value :: xs =>
          if (!isPropertyIterable(value.key.name)) lookupByProps(xs) else
            g.V().has(value).headOption() match {
              case Some(v) => v
              case None => lookupByProps(xs)
            }
      }
    }
    val t0 = System.nanoTime()
    val res = lookupByProps(properties)
    if (res != null) {
      addPropertiesToVertex(res.id.toString)
    }
    logger.debug(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to check if vertex was already in the DB")
    res
  }

  def isPropertyIterable(prop: String): Boolean = {
    def checkOnProps(set: Set[Property]): Boolean = {
      set.toList match {
        case Nil => false
        case x => if (x.head.name == prop) {
          if (x.head.getUniqueness) true else checkOnProps(x.tail.toSet)
        } else {
          checkOnProps(x.tail.toSet)
        }
      }
    }

    checkOnProps(propSet)
  }

  def exist: Boolean = vertex != null

  /**
    * Adds a vertex in the database with his label and properties.
    *
    * @param properties The properties of the to-be-added vertex as a list of gremlin.scala.KeyValues.
    * @param label      The label of the to-be-added vertex.
    * @param b          Bindings for indexing.
    */
  def addVertex(properties: List[KeyValue[String]], label: String, b: Bindings): Unit = {
    if (exist) {
      throw new ImportToGremlinException("Vertex already exist in the database")
    } else {
      try {
        vertex = g.addV(b.of("label", label)).property(properties.head).l().head
        for (keyV <- properties.tail) {
          g.V(vertex.id).property(keyV).iterate()
        }
      } catch {
        case e: CompletionException => throw new ImportToGremlinException(e.getMessage) //TODO: do something
      }
    }
  }

  private def addPropertiesToVertex(id: String): Unit = {
    val t0 = System.nanoTime()
    for (keyV <- properties) {
      if (!doesPropExist(keyV)) {
        g.V(id).property(keyV).iterate()
      }
      logger.debug(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to add vertex ${vertex.id()} to DB")
    }

    def doesPropExist(keyV: KeyValue[String]): Boolean = g.V(id).properties(keyV.key.name).toList().nonEmpty
  }

  /**
    * Returns a Map<Any, List<Any>> fo the properties. A vertex property can have a list of values, thus why
    * the method is returning this kind of structure.
    *
    * @return A map containing the properties name and respective values of the vertex contained in this structure.
    */
  def getPropertiesMap: Map[Any, List[Any]] = {
    val res = g.V(vertex).valueMap.toList().head.asScala.toMap.asInstanceOf[Map[Any, util.ArrayList[Any]]]
    res map { x => x._1 -> x._2.asScala.toList }
  }

  def deleteVertex(): Unit = {
    if (exist) g.V(vertex.id).drop().iterate()
  }

}

