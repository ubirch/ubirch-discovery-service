package com.ubirch.discovery.kafka

import java.util.concurrent.Executors

import com.ubirch.discovery.core.Boot
import com.ubirch.discovery.kafka.consumer.DefaultExpressDiscoveryApp

import scala.concurrent.ExecutionContext

object Service extends Boot with DefaultExpressDiscoveryApp {

  override implicit def ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  override def prefix: String = "Ubirch"

  override def maxTimeAggregationSeconds: Long = 180
}
