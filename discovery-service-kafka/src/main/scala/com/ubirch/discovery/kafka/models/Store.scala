package com.ubirch.discovery.kafka.models

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure.{Relation, VertexCore, VertexDatabase}
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.kafka.util.Exceptions.ParsingException
import gremlin.scala.{Key, KeyValue}

import scala.language.postfixOps

object Store extends LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

  val addVertices = AddRelation()

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
      checkIfLabelIsAllowed(relation.vTo.label)
    //checkIfPropertiesAreAllowed(relation.vFrom.properties) &&
    //checkIfPropertiesAreAllowed(relation.vTo.properties)
    if (!isRelationAllowed) {
      throw ParsingException(s"relation ${relation.toString} is not allowed")
    }
  }

  def vertexToCache(vertexToConvert: VertexCore): VertexDatabase = {
    val vertex = vertexToConvert.toVertexStructDb(gc)
    if (!vertex.existInJanusGraph) vertex.addVertexWithProperties()
    vertex
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

  def addVCached(relation: Relation, vCached: VertexDatabase): Unit = {
    val vertexNotCached = relation.vTo
    val edge = relation.edge
    val isAllowed = checkIfLabelIsAllowed(vertexNotCached.label) // && checkIfPropertiesAreAllowed(vertexNotCached.properties)
    if (!isAllowed) {
      logger.error(s"relation ${relation.toString} is not allowed")
      throw ParsingException(s"relation ${relation.toString} is not allowed")
    }
    addVertices.addTwoVerticesCached(vCached)(vertexNotCached)(edge)
  }

}
