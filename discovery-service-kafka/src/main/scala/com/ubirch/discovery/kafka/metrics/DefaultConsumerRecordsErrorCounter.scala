package com.ubirch.discovery.kafka.metrics

import com.ubirch.kafka.express.ConfigBase
import io.prometheus.client.{Counter => PrometheusCounter, Summary => PrometheusSummary}

trait Counter extends ConfigBase {
  val namespace: String = conf.getString("kafkaApi.metrics.prometheus.namespace")
  val counter: PrometheusCounter
}

trait Summary extends ConfigBase {
  val namespace: String = conf.getString("kafkaApi.metrics.prometheus.namespace")
  val summary: PrometheusSummary
}

//@Singleton
class DefaultConsumerRecordsErrorCounter extends Counter {

  final val counter: PrometheusCounter = PrometheusCounter.build()
    .namespace(namespace)
    .name("failures_count")
    .help("Total storing errors.")
    .labelNames("result")
    .register()

}

//@Singleton
class DefaultConsumerRecordsSuccessCounter extends Counter {

  final val counter: PrometheusCounter = PrometheusCounter.build()
    .namespace(namespace)
    .name("successes_count")
    .help("Total relations stored.")
    .labelNames("result")
    .register()

}

class DefaultMetricsLoggerSummary extends Summary {

  final val summary: PrometheusSummary = PrometheusSummary
    .build(s"processing_time", s"Message processing time in seconds")
    .register()

}
