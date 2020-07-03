package com.ubirch.discovery.models

import com.ubirch.discovery.util.Util

import scala.concurrent.ExecutionContext

/**
  * Only used for parsing kafka messages into core Relations
  */
case class RelationKafka(v_from: VertexKafkaStruct, v_to: VertexKafkaStruct, edge: EdgeKafkaStruct) {
  override def toString: String = {
    s"vFrom: ${v_from.toString} \nvTo: ${v_to.toString} \nedge: ${edge.toString}"
  }

  def toRelationCore(implicit ec: ExecutionContext): Relation = {
    val vFromAsCoreClass = v_from.toVertexCore
    val vToAsCoreClass = v_to.toVertexCore
    val edgeAsCoreClass = edge.toEdgeCore
    Relation(vFromAsCoreClass, vToAsCoreClass, edgeAsCoreClass)
  }
}

case class VertexKafkaStruct(properties: Map[String, Any], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toVertexCore(implicit ec: ExecutionContext): VertexCore = {

    val propertiesToAdd = properties map { kv => Util.convertProp(kv._1, kv._2) }
    VertexCore(propertiesToAdd.toList, label)
  }
}

case class EdgeKafkaStruct(properties: Map[String, Any], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toEdgeCore: EdgeCore = {
    val propertiesToAdd = properties map { kv => Util.convertProp(kv._1, kv._2) }
    EdgeCore(propertiesToAdd.toList, label)
  }
}

