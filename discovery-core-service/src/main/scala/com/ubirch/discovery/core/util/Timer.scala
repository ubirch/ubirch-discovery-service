package com.ubirch.discovery.core.util

import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

object Timer extends LazyLogging {

  case class Timed[R](result: Try[R], timeTaken: Interval, description: String) {
    lazy val elapsed: Long = timeTaken.time
    def logTimeTaken(arg: String = description, criticalTimeMs: Int = 1000, warnOnly: Boolean = true): Unit = {
      if (elapsed > criticalTimeMs) {
        logger.warn(compact(render(logMessage(arg, elapsed, "warn"))))
      } else if (!warnOnly) {
        logger.debug(compact(render(logMessage(arg, elapsed))))
      }
    }

    def logTimeTakenJson(arg: (String, List[JObject]), criticalTimeMs: Int = 1000, warnOnly: Boolean = true): Unit = {
      if (elapsed > criticalTimeMs) {
        logger.warn(compact(render(logMessageJson(arg, elapsed, "warn"))))
      } else if (!warnOnly) {
        logger.debug(compact(render(logMessageJson(arg, elapsed))))
      }
    }
  }

  def time[R](block: => R, description: String = ""): Timed[R] = {
    val t0 = System.currentTimeMillis()
    val result = Try(block)
    val t1 = System.currentTimeMillis()
    Timed(result, Interval(t0, t1), description)
  }

  case class Interval(start: Long, end: Long) {
    def time: Long = end - start
  }

  private def logMessage(arg: String, time: Long, state: String = "ok") = {
    "Timer" ->
      ("timeTaken" -> time) ~
      ("to" -> arg) ~
      ("state" -> state)
  }

  private def logMessageJson(arg: (String, List[JObject]), time: Long, state: String = "ok") = {
    "Timer" ->
      ("timeTaken" -> time) ~
      ("to" -> arg) ~
      ("state" -> state)
  }

}
