package com.ubirch.discovery.kafka.metrics

import com.ubirch.kafka.express.ConfigBase
import io.prometheus.client.{ Counter => PrometheusCounter }

trait Counter extends ConfigBase {
  val namespace: String = conf.getString("kafkaApi.metrics.prometheus.namespace")
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

}

//@Singleton
class DefaultMetricsLoggerCounter extends Counter {

  final val counter: PrometheusCounter = PrometheusCounter.build()
    .namespace(namespace)
    .name("storing_total")
    .help("Total relations stored.")
    .labelNames("result")
    .register()

}
