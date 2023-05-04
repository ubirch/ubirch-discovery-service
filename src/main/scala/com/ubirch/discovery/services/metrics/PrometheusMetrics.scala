package com.ubirch.discovery.services.metrics

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.Lifecycle
import com.ubirch.kafka.express.ConfigBase
import com.ubirch.kafka.metrics.PrometheusMetricsHelper
import io.prometheus.client.exporter.HTTPServer
import javax.inject.{ Inject, Singleton }

import scala.concurrent.Future

@Singleton
class PrometheusMetrics @Inject() (lifecycle: Lifecycle) extends LazyLogging with ConfigBase {

  val port: Int = conf.getInt("kafkaApi.metrics.prometheus.port")

  logger.debug("Creating Prometheus Server on Port[{}]", port)

  val server: HTTPServer = PrometheusMetricsHelper.defaultWithJXM(port)

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Prometheus")
    Future.successful(server.close())
  }

}
