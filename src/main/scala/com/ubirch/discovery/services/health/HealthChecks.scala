package com.ubirch.discovery.services.health

import java.util.Calendar

import com.ubirch.discovery.Lifecycle
import com.ubirch.discovery.models.DefaultHealthAggregator
import javax.inject.{ Inject, Singleton }
import org.scalatra.{ Ok, ScalatraServlet }

@Singleton
class HealthChecks @Inject() (lifecycle: Lifecycle) extends ScalatraServlet {

  val servicesCheckList = List(HealthChecks.JANUSGRAPH, HealthChecks.KAFKA_CONSUMER, HealthChecks.KAFKA_PRODUCER)

  // definis dans helm chart: port et url
  get("/live") {
    Ok("Ok")
  }

  get("/ready") {
    val healthReports = DefaultHealthAggregator.getAllHealth
    if (healthReports.size != servicesCheckList.size) {
      halt(400, s"Not all services are ready, only the following services reported something: ${healthReports.mkString(", ")}")
    } else {
      val failingServices = healthReports.filter(h => !h.isUp)
      if (failingServices.nonEmpty) {
        halt(400, s"Some services are failing -> ${failingServices.mkString(", ")}")
      } else {
        val healthChecksOutdated = healthReports.filter(h => (Calendar.getInstance().getTime.getTime - h.time.getTime) >= 10000L)
        if (healthChecksOutdated.nonEmpty) {
          halt(400, s"Some services have not reported during the last 10 seconds -> ${healthChecksOutdated.mkString(", ")}")
        } else {
          Ok("[" + healthReports.mkString(", ") + "]")
        }
      }
    }
  }

}

object HealthChecks {
  val JANUSGRAPH = "Janusgraph"
  val KAFKA_CONSUMER = "KafkaConsumer"
  val KAFKA_PRODUCER = "KafkaProducer"
}
