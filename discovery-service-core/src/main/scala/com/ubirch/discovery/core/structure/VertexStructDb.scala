package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import gremlin.scala.{Key, KeyValue, TraversalSource}
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class VertexStructDb(val id: String, val g: TraversalSource) extends LazyLogging {

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  val Id: Key[String] = Key[String]("IdAssigned")

  var vertex: Vertex = {
    val t0 = System.nanoTime()
    val res = g.V.has(Id, id).headOption() match {
      case Some(x) => x
      case None => null
    }
    logger.info(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to check if vertex with ID $id was already in the DB")
    res
  }

  def exist: Boolean = if (vertex == null) false else true

  /**
    * Adds a vertex in the database with his label and properties.
    *
    * @param properties The properties of the to-be-added vertex as a list of gremlin.scala.KeyValues.
    * @param label      The label of the to-be-added vertex.
    * @param b          Bindings for indexing.
    */
  def addVertex(properties: List[KeyValue[String]], label: String, b: Bindings): Unit = {
    if (exist) {
      throw new IllegalStateException(s"Vertex ${vertex.id()} already exist in the database")
    } else {
      val t0 = System.nanoTime()
      vertex = g.addV(b.of("label", label)).property(Id -> id).l().head
      for (keyV <- properties) {
        try {
          g.V(vertex.id).property(keyV).iterate()
        } catch {
          case e: CompletionException => throw new ImportToGremlinException(e.getMessage) //TODO: do something
        }
      }
      logger.info(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to add vertex ${vertex.id()} to DB")
    }
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

}

