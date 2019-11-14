package com.ubirch.discovery.core.util

import com.typesafe.scalalogging.LazyLogging

class Timer(event: Event) extends LazyLogging {

  var timeTimerStart: Long = 0

  var events: Map[Event, Interval] = Map.empty

  def init(event: Event): Unit = {
    timeTimerStart = getTime
    start(event)
  }

  private def getTime = System.currentTimeMillis()
  init(event)


  def finish(arg: String): String = {
    val timeTotal = System.currentTimeMillis() - timeTimerStart
    if (timeTotal > 1000) {
      logger.warn(s"Time to do $arg took ${timeTotal.toString} ms!")
    } else {
      logger.debug(s"Took ${timeTotal.toString} ms to $arg")
    }
    timeTotal.toString
  }

  def start(event: Event): Unit = {
    val eventStart = Interval(getTime, null)
    events = events + (event -> eventStart)
  }

  def stop(event: Event) = {
    val r = events.get(event)
  }
}

case class Interval(start: Long, finish: Option[Long])

case class Event(name: String)
