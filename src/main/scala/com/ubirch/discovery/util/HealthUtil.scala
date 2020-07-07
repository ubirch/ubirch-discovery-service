package com.ubirch.discovery.util

import java.util.Calendar

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.models.HealthReport
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.common.{ Metric, MetricName }
import org.json4s.JsonAST._
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object HealthUtil extends LazyLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  /** Extracts relevant kafka metrics and evaluates whether they are healthy. */
  def processKafkaMetrics(name: String, metrics: collection.Map[MetricName, Metric], connectionCountMustBeNonZero: Boolean) = {
    implicit class RichMetric(m: Metric) {
      // we're never interested in consumer node metrics, so let's get rid of them here
      @inline def is(name: String): Boolean = m.metricName().group() != "consumer-node-metrics" && m.metricName().name() == name

      @inline def toTuple: (String, AnyRef) = m.metricName().name() -> m.metricValue()
    }

    // TODO: ask for relevant metrics
    def relevantMetrics(metrics: collection.Map[MetricName, Metric]): Map[String, AnyRef] = {
      metrics.values.collect {
        case m if m is "last-heartbeat-seconds-ago" => m.toTuple
        case m if m is "heartbeat-total" => m.toTuple
        case m if m is "version" => m.toTuple
        case m if m is "request-rate" => m.toTuple
        case m if m is "response-rate" => m.toTuple
        case m if m is "commit-rate" => m.toTuple
        case m if m is "successful-authentication-total" => m.toTuple
        case m if m is "failed-authentication-total" => m.toTuple
        case m if m is "assigned-partitions" => m.toTuple
        case m if m.is("records-consumed-rate") && m.metricName().tags().size() == 1 => m.toTuple
        case m if m is "connection-count" => m.toTuple
        case m if m is "connection-close-total" => m.toTuple
        case m if m is "commit-latency-max" => m.toTuple
      }(collection.breakOut)
    }

    val processedMetrics = relevantMetrics(metrics)
    // TODO: ask for other failure conditions
    val success = {
      val isCountOk =
        !connectionCountMustBeNonZero || processedMetrics.getOrElse("connection-count", 0.0).asInstanceOf[Double] != 0.0
      if (!isCountOk)
        logger.warn(
          s"kafka client ({}) connection-count is zero, but it must be non-zero!",
          v("kafkaClientName", name)
        )
      isCountOk
    } && {
      val lastHeartbeat = processedMetrics.getOrElse("last-heartbeat-seconds-ago", 0.0).asInstanceOf[Double]

      // the second part of that check is needed because kafka uses unreasonable defaults
      // (the value is roughly seconds since 1970, as of 2019-10-28)
      val isLastHeartbeatOk = lastHeartbeat < 60.0 || lastHeartbeat > 49 * 365 * 24 * 60 * 60

      if (!isLastHeartbeatOk)
        logger.warn(
          "last kafka client ({}) heartbeat was {} seconds ago",
          v("kafkaClientName", name), v("heartbeatSecondsAgo", lastHeartbeat)
        )

      isLastHeartbeatOk
    }

    def json(metrics: Map[String, AnyRef]): JValue =
      JsonMethods.fromJsonNode(JsonMethods.mapper.valueToTree[JsonNode](metrics.asJava))

    val payload = write(json(processedMetrics).merge(JObject("status" -> JString(if (success) "ok" else "nok"))))

    HealthReport(name, payload, success, Calendar.getInstance().getTime)
  }

}

/** Health check result. */
case class CheckResult(checkName: String, success: Boolean, payload: JValue)

