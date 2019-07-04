package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.VertexStructDb
import gremlin.scala.{Key, KeyValue}

import scala.language.postfixOps

object Store extends LazyLogging {

  implicit val gc = GremlinConnector.get

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
    * "id": "ID"
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label" OPTIONAL
    * }
    * "v2":{
    * "id": "ID"
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label" OPTIONAL
    * }
    * "edge":{
    * "properties":{
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * "label": "label" OPTIONAL
    * }}}
    *
    * @param req The parsed JSON
    * @return
    */
  def addV(req: AddV): Unit = {
    val id1 = req.v1.id
    val p1 = mapToListKeyValues(req.v1.properties)
    val l1 = req.v1.label
    val id2 = req.v2.id
    val p2 = mapToListKeyValues(req.v2.properties)
    val l2 = req.v2.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    addVertices.addTwoVertices(id1, p1, l1)(id2, p2, l2)(pE, lE)
  }

  def vertexToCache(vertexToConvert: VertexKafkaStruct): VertexStructDb = {
    val vertex = new VertexStructDb(vertexToConvert.id, gc.g)
    // add it to the DB if not already present
    if (!vertex.exist) {
      val pCached = mapToListKeyValues(vertexToConvert.properties)
      val lCached = vertexToConvert.label
      vertex.addVertex(pCached, lCached, gc.b)
    }
    vertex
  }

  def addVCached(req: AddV, vCached: VertexStructDb): Unit = {
    val idNotCached = req.v2.id
    val pNotCached = mapToListKeyValues(req.v2.properties)
    val lNotCached = req.v2.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    addVertices.addTwoVerticesCached(vCached)(idNotCached, pNotCached, lNotCached)(pE, lE)
  }

}
