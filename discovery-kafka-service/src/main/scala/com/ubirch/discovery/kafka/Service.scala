package com.ubirch.discovery.kafka

import com.ubirch.discovery.core.{ Boot, ExecutionContextHelper }
import com.ubirch.discovery.kafka.consumer.DefaultExpressDiscoveryApp

import scala.concurrent.ExecutionContext

object Service extends Boot with DefaultExpressDiscoveryApp {

  override implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180
}
