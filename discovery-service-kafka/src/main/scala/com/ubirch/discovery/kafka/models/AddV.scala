package com.ubirch.discovery.kafka.models

case class AddV(v1: VertexKafkaStruct, v2: VertexKafkaStruct, edge: EdgeKafkaStruct)

case class VertexKafkaStruct(properties: Map[String, String], label: String = "aLabel")

case class EdgeKafkaStruct(properties: Map[String, String], label: String = "aLabel")
