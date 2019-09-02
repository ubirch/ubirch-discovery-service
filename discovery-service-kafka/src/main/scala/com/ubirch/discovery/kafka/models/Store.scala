package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.Elements.{ Label, Property }
import com.ubirch.discovery.core.structure.VertexStructDb
import gremlin.scala.{ Key, KeyValue }

import scala.language.postfixOps

object Store extends LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnector.get

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

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
    val p1 = mapToListKeyValues(req.v_from.properties)
    val l1 = req.v_from.label
    logger.info("l1:" + l1)
    val p2 = mapToListKeyValues(req.v_to.properties)
    val l2 = req.v_to.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    checkIfLabelIsAllowed(l1)
    checkIfLabelIsAllowed(l2)
    checkIfPropertiesAreAllowed(p1)
    checkIfPropertiesAreAllowed(p2)
    addVertices.addTwoVertices(p1, l1)(p2, l2)(pE, lE)
  }

  def vertexToCache(vertexToConvert: VertexKafkaStruct): VertexStructDb = {
    val pCached = mapToListKeyValues(vertexToConvert.properties)
    val vertex = new VertexStructDb(pCached, gc.g, vertexToConvert.label)
    if (!vertex.exist) vertex.addVertex(pCached, vertexToConvert.label, gc.b)
    // add it to the DB if not already present
    vertex
  }

  def checkIfLabelIsAllowed(label: String): Boolean = {
    def iterate(it: List[Label]): Boolean = {
      it match {
        case Nil => false
        case x :: xs => if (x.name.equals(label)) true else iterate(xs)
      }
    }

    iterate(KafkaElements.listOfAllLabels.toList)
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
        case x :: xs => if (iterate(KafkaElements.listOfAllProperties.toList, x.key.name)) checkAllProps(xs) else false
      }
    }

    checkAllProps(props)
  }

  def addVCached(req: AddV, vCached: VertexStructDb): Unit = {
    val pNotCached = mapToListKeyValues(req.v_to.properties)
    val lNotCached = req.v_to.label
    val pE = mapToListKeyValues(req.edge.properties)
    val lE = req.edge.label
    checkIfLabelIsAllowed(lNotCached)
    checkIfPropertiesAreAllowed(pNotCached)
    addVertices.addTwoVerticesCached(vCached)(pNotCached, lNotCached)(pE, lE)
  }

}
