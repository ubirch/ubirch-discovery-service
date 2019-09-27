package com.ubirch.discovery.kafka.models

import com.ubirch.discovery.core.structure.{EdgeCore, Relation, VertexCore}
import gremlin.scala.{Key, KeyValue}

case class AddV(v_from: VertexKafkaStruct, v_to: VertexKafkaStruct, edge: EdgeKafkaStruct) {
  override def toString: String = {
    s"vFrom: ${v_from.toString} \n vTo: ${v_to.toString} \n edge: ${edge.toString}"
  }

  def toCoreRelation: Relation = {
    val vFromAsCoreClass = v_from.toVertexToAdd
    val vToAsCoreClass = v_to.toVertexToAdd
    val edgeAsCoreClass = edge.toEdgeToAdd
    Relation(vFromAsCoreClass, vToAsCoreClass, edgeAsCoreClass)
  }
}

case class VertexKafkaStruct(properties: Map[String, String], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toVertexToAdd: VertexCore = {
    val propertiesToAdd = properties map { kv => new KeyValue[String](Key(kv._1), kv._2) }
    VertexCore(propertiesToAdd.toList, label)
  }
}

case class EdgeKafkaStruct(properties: Map[String, String], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }

  def toEdgeToAdd: EdgeCore = {
    val propertiesToAdd = properties map { kv => new KeyValue[String](Key(kv._1), kv._2) }
    EdgeCore(propertiesToAdd.toList, label)
  }
}
