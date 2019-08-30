package com.ubirch.discovery.kafka.models

case class AddV(v_from: VertexKafkaStruct, v_to: VertexKafkaStruct, edge: EdgeKafkaStruct)

case class VertexKafkaStruct(properties: Map[String, String], label: String = "aLabel")

case class EdgeKafkaStruct(properties: Map[String, String], label: String = "aLabel")
