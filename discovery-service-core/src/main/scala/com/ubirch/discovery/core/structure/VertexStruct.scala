package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class VertexStruct(label: String, properties: Map[String, String]) extends LazyLogging {

  override def toString: String = {
    var s: String = s"Label: $label"
    for ((k, v) <- properties) {
      logger.info("k: " + k)
      s += s"\n$k: ${v.toString}"
      logger.info("end")
    }
    s
  }

  def toJson: String = {
    val json = ("label" -> this.label) ~ ("properties" -> properties)
    compact(render(json))
  }

}
