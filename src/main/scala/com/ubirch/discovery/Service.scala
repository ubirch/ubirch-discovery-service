package com.ubirch.discovery

import com.ubirch.discovery.consumer.DefaultExpressDiscoveryApp
import com.ubirch.discovery.util.ExecutionContextHelper

import scala.concurrent.ExecutionContext

object Service extends Boot with DefaultExpressDiscoveryApp {

  override implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180
}
