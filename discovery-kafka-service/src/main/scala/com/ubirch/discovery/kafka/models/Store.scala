package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure.{Relation, VertexCore, VertexDatabase}
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.PrometheusRelationMetricsLoggerSummary
import com.ubirch.discovery.kafka.util.Exceptions.ParsingException
import gremlin.scala.{Key, KeyValue}
import io.prometheus.client.Summary

import scala.language.postfixOps
import scala.util.Try

object Store extends LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

  val addVertices = AddRelation()
  val relationTimeSummary = new PrometheusRelationMetricsLoggerSummary

  /**
    * Transforms a map[String, String] to a list of KeyValue[String].
    *
    * @param propMaps The map that'll be transformed.
    * @return The List[KeyValue] corresponding to the Map passed as a parameter.
    */
  def mapToListKeyValues(propMaps: Map[String, String]): List[KeyValue[String]] = propMaps map { x => KeyValue(Key(x._1), x._2) } toList

  /**
    * Entry should be formatted as the following:
    * {"v1":{
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label"
    * }
    * "v2":{ same }
    * "edge":{ same }
    * }}
    *
    * @param relation The parsed JSON
    * @return
    */
  def addRelation(relation: Relation): Try[String] = {
    val requestTimer: Summary.Timer = relationTimeSummary.summary
      .labels("relation_process_time")
      .startTimer

    val res = Timer.time({
      stopIfRelationNotAllowed(relation)
      addVertices.createRelation(relation)
    })
    relationTimeSummary.summary.observe(requestTimer.observeDuration())

    res.logTimeTakenJson("inscribe relation" -> List(relation.toJson))
    res.result

  }

  def addRelationOneCached(relation: Relation, vCached: VertexDatabase): Try[String] = {

    val requestTimer: Summary.Timer = relationTimeSummary.summary
      .labels("relation_process_time")
      .startTimer

    val res = Timer.time({
      val vertexNotCached = relation.vTo
      val edge = relation.edge
      val isAllowed = checkIfLabelIsAllowed(vertexNotCached.label) // && checkIfPropertiesAreAllowed(vertexNotCached.properties)
      if (!isAllowed) {
        logger.error(s"relation ${relation.toString} is not allowed")
        throw ParsingException(s"relation ${relation.toString} is not allowed")
      }
      addVertices.createRelationOneCached(vCached)(vertexNotCached)(edge)
    })

    relationTimeSummary.summary.observe(requestTimer.observeDuration())
    res.logTimeTakenJson("inscribe relation" -> List(relation.toJson))
    res.result

  }

  def stopIfRelationNotAllowed(relation: Relation): Unit = {
    val isRelationAllowed = checkIfLabelIsAllowed(relation.vFrom.label) &&
      checkIfLabelIsAllowed(relation.vTo.label)
    if (!isRelationAllowed) {
      throw ParsingException(s"relation ${relation.toString} is not allowed")
    }
  }

  def vertexToCache(vertexToConvert: VertexCore): VertexDatabase = {
    val vertex = vertexToConvert.toVertexStructDb(gc)
    if (!vertex.existInJanusGraph) vertex.addVertexWithProperties() else vertex.update()
    vertex
  }

  def addVertex(vertex: VertexCore): Unit = {
    val vDb = vertex.toVertexStructDb(gc)
    if (!vDb.existInJanusGraph) {
      vDb.addVertexWithProperties()
    } else {
      vDb.update()
    }

  }

  def checkIfLabelIsAllowed(label: String): Boolean = {
    KafkaElements.labelsAllowed.exists(e => e.name.equals(label))
  }

  def checkIfPropertiesAreAllowed(props: List[KeyValue[String]]): Boolean = {
    def iterate(it: List[Property], prop: String): Boolean = {
      it match {
        case Nil => false
        case x :: xs => if (x.name.equals(prop)) true else iterate(xs, prop)
      }
    }

    def checkAllProps(it: List[KeyValue[String]]): Boolean = {
      it match {
        case Nil => true
        case x :: xs => if (iterate(KafkaElements.propertiesAllowed.toList, x.key.name)) checkAllProps(xs) else false
      }
    }

    checkAllProps(props)
  }

  /**
    * This helper is here to speedup subsequent relations processing and avoid asynchronous collision error in JanusGraph
    * (ie: two executor tries to update the same vertex)
    */
  def addVerticesPresentMultipleTimes(relations: List[Relation]): Unit = {
    val verticesPresentMultipleTimes = getVerticesPresentMultipleTime(relations)
    verticesPresentMultipleTimes.foreach { v => Store.addVertex(v) }
  }

  def getVerticesPresentMultipleTime(relations: List[Relation]): Set[VertexCore] = {
    val vertices = relations.flatMap(r => List(r.vFrom, r.vTo))

    // check for vertices who have the same unique properties but are not "exactly" equal
    val verticesCheck1: Set[VertexCore] = vertices.toSet
    val resCheck1 = verticesCheck1.filter { v =>
      (verticesCheck1 - v) exists { v2 => v2.equalsUniqueProperty(v) }
    }

    // general equality check
    val resCheck2 = vertices.groupBy(identity).collect { case (v, List(_, _, _*)) => v }.toSet

    val resTotal = resCheck1 ++ resCheck2
    logger.debug(s"vertices duplicates: ${resTotal.map { v => v.toString }}")
    resTotal
  }

}
