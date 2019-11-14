package com.ubirch.discovery.core.util

import com.typesafe.scalalogging.LazyLogging

class Timer() extends LazyLogging {

  var timeTimerStart: Long = 0

  def init(): Unit = {
    timeTimerStart = System.currentTimeMillis()
  }

  init()

  def finish(arg: String): String = {
    val timeTotal = System.currentTimeMillis() - timeTimerStart
    if (timeTotal > 1000) {
      logger.warn(s"Time to do $arg took ${timeTotal.toString} ms!")
    } else {
      logger.debug(s"Took ${timeTotal.toString} ms to $arg")
    }
    timeTotal.toString
  }
}
