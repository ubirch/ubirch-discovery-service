package com.ubirch.discovery.kafka.models

import com.ubirch.discovery.core.structure.{ EdgeCore, Relation, VertexCore }
import com.ubirch.discovery.kafka.util.Util._

import scala.concurrent.ExecutionContext

/**
  * Only used for parsing kafka messages into core Relations
  */
case class RelationKafka(v_from: VertexKafkaStruct, v_to: VertexKafkaStruct, edge: EdgeKafkaStruct) {
  override def toString: String = {
    s"vFrom: ${v_from.toString} \nvTo: ${v_to.toString} \nedge: ${edge.toString}"
  }

  def toCoreRelation(implicit ec: ExecutionContext): Relation = {
    val vFromAsCoreClass = v_from.toVertexToAdd
    val vToAsCoreClass = v_to.toVertexToAdd
    val edgeAsCoreClass = edge.toEdgeToAdd
    Relation(vFromAsCoreClass, vToAsCoreClass, edgeAsCoreClass)
  }
}

case class VertexKafkaStruct(properties: Map[String, Any], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toVertexToAdd(implicit ec: ExecutionContext): VertexCore = {

    val propertiesToAdd = properties map { kv => convertProp(kv._1, kv._2) }
    VertexCore(propertiesToAdd.toList, label)
  }
}

case class EdgeKafkaStruct(properties: Map[String, Any], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toEdgeToAdd: EdgeCore = {
    val propertiesToAdd = properties map { kv => convertProp(kv._1, kv._2) }
    EdgeCore(propertiesToAdd.toList, label)
  }
}

