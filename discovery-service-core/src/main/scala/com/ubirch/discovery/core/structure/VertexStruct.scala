package com.ubirch.discovery.core.structure

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{ Logger, LoggerFactory }

case class VertexStruct(label: String, properties: Map[String, String]) {

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  override def toString: String = {
    var s: String = s"Label: $label"
    for ((k, v) <- properties) {
      log.info("k: " + k)
      s += s"\n$k: ${v.toString}"
      log.info("end")
    }
    s
  }

  def toJson: String = {
    val json = ("label" -> this.label) ~ ("properties" -> properties)
    compact(render(json))
  }

}
