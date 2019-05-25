package com.ubirch.discovery.kafka.models

case class AddV(v1: Vertounet, v2: Vertounet, edge: Edgounet)

case class Vertounet(id: String, properties: Map[String, String], label: String = "aLabel")

case class Edgounet(properties: Map[String, String])