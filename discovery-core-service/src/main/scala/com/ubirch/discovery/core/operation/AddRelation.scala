package com.ubirch.discovery.core.operation

import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.util.Exceptions.{ImportToGremlinException, KeyNotInList, PropertiesNotCorrect}
import com.ubirch.discovery.core.util.Util.{getEdge, getEdgeProperties, recompose}
import com.ubirch.discovery.core.util.Util
import gremlin.scala.Edge
import org.janusgraph.core.SchemaViolationException

import scala.concurrent.{ExecutionContext, Future}
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
        else Util.getOneEdge(relation.vFromDb, relation.vFromDb)
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

}
