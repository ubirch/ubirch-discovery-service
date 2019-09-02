package com.ubirch.discovery.kafka.models

case class AddV(v_from: VertexKafkaStruct, v_to: VertexKafkaStruct, edge: EdgeKafkaStruct) {
  override def toString: String = {
    s"vFrom: ${v_from.toString} \n vTo: ${v_to.toString} \n edge: ${edge.toString}"
  }
}

case class VertexKafkaStruct(properties: Map[String, String], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }
}

case class EdgeKafkaStruct(properties: Map[String, String], label: String = "aLabel") {
  override def toString: String = {
    s"label: $label; properties: ${properties.mkString(", ")}"
  }
}
