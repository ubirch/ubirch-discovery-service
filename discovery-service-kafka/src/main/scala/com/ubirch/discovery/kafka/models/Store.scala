package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.VertexStructDb
import gremlin.scala.{Key, KeyValue}

import scala.language.postfixOps

object Store extends LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnector.get

  val addVertices = AddVertices()

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
    * "v2":{
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label"
    * }
    * "edge":{
    * "properties":{
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label"
    * }}}
    *
    * @param req The parsed JSON
    * @return
    */
  def addV(req: AddV): Unit = {
    val p1 = mapToListKeyValues(req.v1.properties)
    val l1 = req.v1.label
    logger.info("l1:" + l1)
    val p2 = mapToListKeyValues(req.v2.properties)
    val l2 = req.v2.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    addVertices.addTwoVertices(p1, l1)(p2, l2)(pE, lE)
  }

  def vertexToCache(vertexToConvert: VertexKafkaStruct): VertexStructDb = {
    val pCached = mapToListKeyValues(vertexToConvert.properties)
    val vertex = new VertexStructDb(pCached, gc.g, vertexToConvert.label)
    if (!vertex.exist) vertex.addVertex(pCached, vertexToConvert.label, gc.b)
    // add it to the DB if not already present
    vertex
  }

  def addVCached(req: AddV, vCached: VertexStructDb): Unit = {
    val pNotCached = mapToListKeyValues(req.v2.properties)
    val lNotCached = req.v2.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    addVertices.addTwoVerticesCached(vCached)(pNotCached, lNotCached)(pE, lE)
  }

}
