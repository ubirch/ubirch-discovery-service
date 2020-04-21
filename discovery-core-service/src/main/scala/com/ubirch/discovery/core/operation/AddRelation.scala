package com.ubirch.discovery.core.operation

import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.util.Exceptions.{ ImportToGremlinException, KeyNotInList, PropertiesNotCorrect }
import com.ubirch.discovery.core.util.Util.{ getEdge, getEdgeProperties, recompose }
import gremlin.scala.Edge
import org.janusgraph.core.SchemaViolationException

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

/**
  * Allows the storage of two nodes (vertices) in the janusgraph server. Link them together
  */
object AddRelation extends LazyLogging {

  private val label = "aLabel"

  def twoExistCache(relation: RelationServer)(implicit propSet: Set[Property], gc: GremlinConnector, ec: ExecutionContext): Future[Option[Edge]] = {
    //logger.debug(Util.relationStrategyJson(relation, "two exist"))

    def recoverEdge(error: Throwable) = {
      areVertexLinked(relation.vFromDb, relation.vToDb).flatMap { linked =>
        if (!linked) relation.createEdge
        else Future.failed(new Exception("Error creating edge"))
      }
    }

    relation.createEdge.recoverWith {
      case e: CompletionException => recoverEdge(e)
      case e: SchemaViolationException => recoverEdge(e)
    }.recoverWith {
      case e: Exception =>
        logger.error("error initialising vertex", e)
        Future.failed(e)
    }

  }

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  def areVertexLinked(vFrom: VertexDatabase, vTo: VertexDatabase)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Boolean] = {
    gc.g.V(vFrom.vertex).both().is(vTo.vertex).promise().map(_.nonEmpty)
  }

  /**
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
  def verifVertex(vertexStruct: VertexDatabase, properties: List[ElementProperty], l: String = label): Unit = {

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

  /**
    * Verify if an edge has been correctly added to the janusgraph server.
    *
    * @param vFrom      Id of the vertex from where the edge originate.
    * @param vTo        Id of the vertex to where the edge goes.
    * @param properties properties of the edge.
    */
  def verifEdge(vFrom: VertexDatabase, vTo: VertexDatabase, properties: List[ElementProperty])(implicit gc: GremlinConnector): Unit = {
    val edge = getEdge(gc, vFrom, vTo).head

    if (edge == null) throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't created")

    val keyList = properties map (x => x.keyName)
    val propertiesInServer = try {
      recompose(getEdgeProperties(gc, edge), keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
      case x: Throwable => throw x
    }

    if (!(propertiesInServer.sortBy(x => x.keyName) == properties.sortBy(x => x.keyName)))
      throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
  }

  def stopIfVerticesAreEquals(vertex1: VertexCore, vertex2: VertexCore): Unit = {
    if (vertex1 equals vertex2) {
      throw PropertiesNotCorrect(s"p1 = ${vertex1.properties.map(x => s"${x.keyName} = ${x.value}, ")} should not be equal to the properties of the second vertex")
    }
  }
}
