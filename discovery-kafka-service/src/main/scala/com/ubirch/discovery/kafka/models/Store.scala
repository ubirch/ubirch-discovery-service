package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure.{ Relation, RelationServer }
import com.ubirch.discovery.kafka.metrics.PrometheusRelationMetricsLoggerSummary
import gremlin.scala.Edge

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

/**
  * This object calls function from the core library
  */
object Store extends LazyLogging {

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

  val relationTimeSummary = new PrometheusRelationMetricsLoggerSummary

  def addRelationTwoCached(relation: RelationServer)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Option[Edge]] = {
    AddRelation.twoExistCache(relation)
  }

  def getAllVerticeFromRelations(relations: Seq[Relation]) = {
    relations.flatMap(r => List(r.vFrom, r.vTo)).distinct
  }

}
