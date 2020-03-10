package com.ubirch.discovery.kafka

import java.util.concurrent.Executors

import com.ubirch.discovery.core.Boot
import com.ubirch.discovery.kafka.consumer.DefaultExpressDiscoveryApp

import scala.concurrent.ExecutionContext

object Service extends Boot with DefaultExpressDiscoveryApp {

  override implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180
}
