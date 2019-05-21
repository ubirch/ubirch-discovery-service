package com.ubirch.discovery.core.util

import com.ubirch.discovery.core.structure.VertexStruct
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, JsonAST}
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

object Util {

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  def arrayVertexToJson(arrayVertexes: Array[VertexStruct]): String = {

    implicit def vertexes2JValue(v: VertexStruct): JsonAST.JObject = {
      ("label" -> v.label) ~ ("properties" -> v.properties)
    }

    val json = "list of vertexes" -> reformatArrayVertex(arrayVertexes).toList
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    Serialization.write(json)
  }

  def reformatArrayVertex(arrayVertex: Array[VertexStruct]): Array[VertexStruct] = {
    val arrayVertexReformated: Array[VertexStruct] = new Array(arrayVertex.length)
    var i = 0
    for (v <- arrayVertex) {
      val label = v.label
      val properties: Map[String, String] = v.properties
      var propertiesReformated: Map[String, String] = Map()
      for ((key, value) <- properties) propertiesReformated += (key.toString -> value.toString.substring(1, value.length - 1))

      val vertexReformated: VertexStruct = VertexStruct(label, propertiesReformated)

      arrayVertexReformated(i) = vertexReformated
      i = i + 1
    }

    arrayVertexReformated.foreach(v => log.info(v.toString))
    arrayVertexReformated
  }

}

