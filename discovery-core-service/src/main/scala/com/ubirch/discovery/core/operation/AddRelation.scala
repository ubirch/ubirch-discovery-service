package com.ubirch.discovery.core.operation

import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.{ ImportToGremlinException, KeyNotInList, PropertiesNotCorrect }
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.core.util.Util.{ getEdge, getEdgeProperties, recompose }
import org.janusgraph.core.SchemaViolationException

import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

/**
  * Allows the storage of two nodes (vertices) in the janusgraph server. Link them together
  */
object AddRelation extends LazyLogging {

  private val label = "aLabel"

  def twoExistCache(relation: RelationServer)(implicit propSet: Set[Property], gc: GremlinConnector) = {
    //logger.debug(Util.relationStrategyJson(relation, "two exist"))

    relation.createEdge match {
      case Success(edge) => edge
      case Failure(fail: Exception) => fail match {
        case e: CompletionException => recoverEdge(e)
        case e: SchemaViolationException => recoverEdge(e)
        case e: Exception =>
          logger.error("error initialising vertex", e)
          throw e
      }
    }

    def recoverEdge(error: Throwable) {
      logger.debug("exception thrown while adding edge: " + error.getMessage)
      if (!areVertexLinked(relation.vFromDb, relation.vToDb)) {
        logger.debug("vertices where not linked, creating edge")
        relation.createEdge.get
      }

    }

  }

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  def areVertexLinked(vFrom: VertexDatabase, vTo: VertexDatabase)(implicit gc: GremlinConnector): Boolean = {
    val timedResult = Timer.time(gc.g.V(vFrom.vertex).both().is(vTo.vertex).l())
    timedResult.result match {
      case Success(value) =>
        //timedResult.logTimeTaken(s"check if vertices ${vFrom.vertex.id} and ${vTo.vertex.id} were linked. Result: ${value.nonEmpty}", criticalTimeMs = 100)
        value.nonEmpty
      case Failure(exception) =>
        logger.error("Couldn't check if vertex is linked, defaulting to false := ", exception)
        false
    }
  }

  /**
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
  def verifVertex(vertexStruct: VertexDatabase, properties: List[ElementProperty], l: String = label): Unit = {
    if (!vertexStruct.existInJanusGraph) throw new ImportToGremlinException("Vertex wasn't imported to the Gremlin Server")

    val keyList = properties.map(x => x.keyName)
    val propertiesInServer = vertexStruct.getPropertiesMap
    val propertiesInServerAsListKV = try {
      recompose(propertiesInServer, keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServerAsListKV.sortBy(p => p.keyName) == properties.sortBy(p => p.keyName))) {
      logger.debug(s"properties = ${properties.mkString(", ")}")
      logger.debug(s"propertiesInServer = ${propertiesInServerAsListKV.mkString(", ")}")
      throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
    }
  }

}
