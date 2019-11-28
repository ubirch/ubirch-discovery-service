package com.ubirch.discovery.core.metrics

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.kafka.express.ConfigBase
import com.ubirch.kafka.metrics.PrometheusMetricsHelper
import io.prometheus.client.exporter.HTTPServer

import scala.concurrent.Future

object PrometheusMetrics {
  private val instance = new PrometheusMetrics(Lifecycle.get)
  def get: PrometheusMetrics = instance
}

class PrometheusMetrics(lifecycle: Lifecycle) extends LazyLogging with ConfigBase {

  val port: Int = conf.getInt("core.metrics.prometheus.port")

  logger.debug("Creating Prometheus Server on Port[{}]", port)

  val server: HTTPServer = PrometheusMetricsHelper.create(port)

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Prometheus")
    Future.successful(server.stop())
  }

}
