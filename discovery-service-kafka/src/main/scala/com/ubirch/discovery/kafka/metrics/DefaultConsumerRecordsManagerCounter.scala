package com.ubirch.discovery.kafka.metrics

import io.prometheus.client.{Counter => PrometheusCounter}

trait Counter {
  val namespace: String
  val counter: PrometheusCounter
}

//@Singleton
class DefaultConsumerRecordsManagerCounter extends Counter {

  final val counter: PrometheusCounter = PrometheusCounter.build()
    .namespace(namespace)
    .name("storing_error_total")
    .help("Total storing errors.")
    .labelNames("result")
    .register()
  val namespace: String = "ubirch"

}

//@Singleton
class DefaultMetricsLoggerCounter extends Counter {

  final val counter: PrometheusCounter = PrometheusCounter.build()
    .namespace(namespace)
    .name("storing_total")
    .help("Total relations stored.")
    .labelNames("result")
    .register()
  val namespace: String = "ubirch"

}
