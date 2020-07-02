package com.ubirch.discovery.models

import java.util.Date

import scala.collection.concurrent.TrieMap

trait HealthAggregator {

  val healthAllService: TrieMap[String, HealthReport]

  def updateHealth(className: String, healthReport: HealthReport)

  def getHealth(className: String): Option[HealthReport]

  def getAllHealth: List[HealthReport]

}

object DefaultHealthAggregator extends HealthAggregator {

  override val healthAllService: TrieMap[String, HealthReport] = scala.collection.concurrent.TrieMap.empty

  override def updateHealth(className: String, healthReport: HealthReport): Unit = {
    healthAllService += (className -> healthReport)
  }

  override def getHealth(className: String): Option[HealthReport] = {
    healthAllService.get(className)
  }

  override def getAllHealth: List[HealthReport] = {
    healthAllService.toList.map(h => h._2)
  }
}

case class HealthReport(className: String, value: String, isUp: Boolean, time: Date) {
  override def toString: String = {
    "{\"healthOf\":\"" + className + "\"," +
      "\"value\":\"" + value + "\"," +
      "\"isUp\":" + isUp + "," +
      "\"time\":\"" + time.toString + "\"}"
  }
}
