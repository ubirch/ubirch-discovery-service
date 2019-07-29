package com.ubirch.discovery.kafka.models

import com.ubirch.discovery.core.structure.Elements._

object KafkaElements {


  val propertiesToIterate: Set[Property] = Set(SIGNATURE, HASH, DEVICE_ID)
  val listOfAllProperties: Set[Property] = Set(SIGNATURE, TYPE, HASH, DEVICE_ID)
  val listOfAllLabels: Set[Label] = Set(DEVICE, UPP, ROOT_TREE, FOUNDATION_TREE, BLOCKCHAIN)

  /* Define properties type */
  case object SIGNATURE extends Property("signature", true)

  case object TYPE extends Property("type")

  case object HASH extends Property("hash", true)

  case object DEVICE_ID extends Property("device-id", true)

  /* Define label type */
  case object DEVICE extends Label("device")

  case object UPP extends Label("upp")

  case object ROOT_TREE extends Label("root_tree")

  case object FOUNDATION_TREE extends Label("foundation_tree")

  case object BLOCKCHAIN extends Label("blockchain")

}

