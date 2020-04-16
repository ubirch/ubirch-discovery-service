package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure.{ Relation, RelationServer, VertexCore, VertexDatabase }
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.PrometheusRelationMetricsLoggerSummary
import com.ubirch.discovery.kafka.util.Exceptions.ParsingException
import gremlin.scala.{ Key, KeyValue }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scala.util.Try

/**
  * This object calls function from the core library
  */
object Store extends LazyLogging {

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

  val relationTimeSummary = new PrometheusRelationMetricsLoggerSummary

  def addRelationTwoCached(relation: RelationServer)(implicit gc: GremlinConnector): Try[Unit] = {

    val res: Timer.Timed[Unit] = Timer.time({
      AddRelation.twoExistCache(relation)
    })

    Try(relationTimeSummary.summary.observe(res.elapsed))

    res.logTimeTakenJson("inscribe relation" -> List(relation.toJson), 300)
    res.result

  }

  def addVertex(vertex: VertexCore)(implicit gc: GremlinConnector): VertexDatabase = {
    val vDb = vertex.toVertexStructDb(gc)
    if (!vDb.existInJanusGraph) {
      vDb.addVertexWithProperties()
      vDb
    } else {
      vDb.update()
      vDb
    }

  }

  def checkIfLabelIsAllowed(label: String): Boolean = {
    KafkaElements.labelsAllowed.exists(e => e.name.equals(label))
  }

  def getAllVerticeFromRelations(relations: Seq[Relation]) = {
    relations.flatMap(r => List(r.vFrom, r.vTo)).distinct
  }

  /*
  *
    * Transforms a map[String, String] to a list of KeyValue[String].
    *
    * @param propMaps The map that'll be transformed.
    * @return The List[KeyValue] corresponding to the Map passed as a parameter.
*/
  //def mapToListKeyValues(propMaps: Map[String, String]): List[KeyValue[String]] = propMaps map { x => KeyValue(Key(x._1), x._2) } toList

  /*
  *
    * Entry should be formatted as the following:
    * {"v1":{
    *   "properties": {
    *     "prop1Name": prop1Value,
    *     ...
    *     "propNName": propNValue }
    *   "label": "label" }
    *   "v2":{ same }
    *   "edge":{ same }
    * }}
    *
    * @param relation The parsed JSON
    * @return
*/
  /*def addRelation(relation: Relation)(implicit gc: GremlinConnector): Try[Unit] = {

    val res = Timer.time({
      stopIfRelationNotAllowed(relation)
      AddRelation.createRelation(relation)
    })

    Try(relationTimeSummary.summary.observe(res.elapsed))

    res.logTimeTakenJson("inscribe relation" -> List(relation.toJson), 300)
    res.result.get

  }
*/

  /*  def stopIfRelationNotAllowed(relation: Relation): Unit = {
    val isRelationAllowed = checkIfLabelIsAllowed(relation.vFrom.label) && checkIfLabelIsAllowed(relation.vTo.label)
    if (!isRelationAllowed) throw ParsingException(s"relation ${relation.toString} is not allowed")
  }

  def vertexToCache(vertexToConvert: VertexCore)(implicit gc: GremlinConnector): VertexDatabase = {
    val vertex = vertexToConvert.toVertexStructDb(gc)
    if (!vertex.existInJanusGraph) vertex.addVertexWithProperties() else vertex.update()
    vertex
  }*/

  /*def addRelationOneCached(relation: Relation, vCached: VertexDatabase)(implicit gc: GremlinConnector): Try[Unit] = {

    val res = Timer.time({
      val vertexNotCached = relation.vTo
      val edge = relation.edge
      stopIfRelationNotAllowed(relation)
      AddRelation.createRelationOneCached(vCached)(vertexNotCached)(edge)
    })

    Try(relationTimeSummary.summary.observe(res.elapsed))

    res.logTimeTakenJson("inscribe relation" -> List(relation.toJson), 300)
    res.result.get

  }*/

  /*def checkIfPropertiesAreAllowed(props: List[KeyValue[String]]): Boolean = {

    @tailrec
    def iterate(it: List[Property], prop: String): Boolean = {
      it match {
        case Nil => false
        case x :: xs => if (x.name.equals(prop)) true else iterate(xs, prop)
      }
    }

    @tailrec
    def checkAllProps(it: List[KeyValue[String]]): Boolean = {
      it match {
        case Nil => true
        case x :: xs => if (iterate(KafkaElements.propertiesAllowed.toList, x.key.name)) checkAllProps(xs) else false
      }
    }

    checkAllProps(props)
  }*/

  /**
    * This helper is here to speedup subsequent relations processing and avoid asynchronous collision error in JanusGraph
    * (ie: two executor tries to update the same vertex)
    */
  /*def addVerticesPresentMultipleTimes(relations: List[Relation])(implicit gc: GremlinConnector, ec: ExecutionContext): Unit = {
    val verticesPresentMultipleTimes: Set[VertexCore] = getVerticesPresentMultipleTime(relations)
    Timer.time({
      if (verticesPresentMultipleTimes.size > 2) {
        val executor = new Executor[VertexCore, VertexDatabase](objects = verticesPresentMultipleTimes.map { v => (v, Store.addVertex(_)) }.toSeq, processSize = 16)
        executor.startProcessing()
        executor.latch.await()
      } else verticesPresentMultipleTimes.foreach { v => Store.addVertex(v) }
    }).logTimeTaken(s"add_${verticesPresentMultipleTimes.size}_vertices_present_multiple_times", verticesPresentMultipleTimes.size * 100, warnOnly = false)
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
    resTotal
  }*/

}
