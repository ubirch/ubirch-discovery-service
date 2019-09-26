package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.{Relation, VertexStructDb, VertexToAdd}
import com.ubirch.discovery.core.structure.Elements.{Label, Property}
import com.ubirch.discovery.kafka.util.Exceptions.ParsingException
import gremlin.scala.{Key, KeyValue}

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
    * @param relation The parsed JSON
    * @return
    */
  def addV(relation: Relation): Unit = {
    logger.debug("l1:" + relation.vFrom.label)
    stopIfRelationNotAllowed(relation)
    addVertices.createRelation(relation)
  }

  def stopIfRelationNotAllowed(relation: Relation): Unit = {
    val isRelationAllowed = checkIfLabelIsAllowed(relation.vFrom.label) &&
    checkIfLabelIsAllowed(relation.vTo.label) &&
    checkIfPropertiesAreAllowed(relation.vFrom.properties) &&
    checkIfPropertiesAreAllowed(relation.vTo.properties)
    if (!isRelationAllowed) {
      logger.error(s"relation ${relation.toString} is not allowed")
      throw ParsingException(s"relation ${relation.toString} is not allowed")
    }
  }

  def vertexToCache(vertexToConvert: VertexToAdd): VertexStructDb = {
    val vertex = vertexToConvert.toVertexStructDb(gc.g)
    if (!vertex.existInJanusGraph) vertex.addVertexWithProperties(gc.b)
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

  def addVCached(relation: Relation, vCached: VertexStructDb): Unit = {
    val vertexNotCached = relation.vTo
    val edge = relation.edge
    val isAllowed = checkIfLabelIsAllowed(vertexNotCached.label) && checkIfPropertiesAreAllowed(vertexNotCached.properties)
    if (!isAllowed)  {
      logger.error(s"relation ${relation.toString} is not allowed")
      throw ParsingException(s"relation ${relation.toString} is not allowed")
    }
    addVertices.addTwoVerticesCached(vCached)(vertexNotCached)(edge)
  }

}
