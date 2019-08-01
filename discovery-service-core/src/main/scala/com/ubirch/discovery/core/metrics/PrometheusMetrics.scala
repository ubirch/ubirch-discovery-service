package com.ubirch.discovery.core.metrics

import java.net.BindException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.kafka.express.ConfigBase
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

import scala.concurrent.Future

object PrometheusMetrics {
  private val instance = new PrometheusMetrics(Lifecycle.get)

  def get: PrometheusMetrics = instance
}

class PrometheusMetrics(lifecycle: Lifecycle) extends LazyLogging with ConfigBase {

  DefaultExports.initialize()

  val port: Int = conf.getInt("core.metrics.prometheus.port")

  logger.info("Creating Prometheus Server on Port[{}]", port)

  val server: HTTPServer = try {
    new HTTPServer(port)
  } catch {
    case _: BindException =>
      val newPort = port + new scala.util.Random().nextInt(50)
      logger.info("Port[{}] is busy, trying Port[{}]", port, newPort)
      new HTTPServer(newPort)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Prometheus")
    Future.successful(server.stop())
  }

}
