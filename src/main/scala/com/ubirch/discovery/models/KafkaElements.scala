package com.ubirch.discovery.models

import Elements._

object KafkaElements {

  val propertiesToIterate: Set[Property] = Set(SIGNATURE, HASH, DEVICE_ID)

  /* Define properties type */
  case object SIGNATURE extends Property("signature", true)
  case object TYPE extends Property("type")
  case object BLOCKCHAIN_TYPE extends Property("blockchain")
  case object HASH extends Property("hash", true)
  case object DEVICE_ID extends Property("device_id", true)
  case object TIMESTAMP extends Property("timestamp")

}

