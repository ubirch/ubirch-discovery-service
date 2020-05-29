package com.ubirch.discovery.core.util

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.structure.PropertyType.PropertyType
import com.ubirch.discovery.core.util.Exceptions.{ KeyNotInList, NumberOfEdgesNotCorrect }
import gremlin.scala.{ Key, KeyValue }
import org.apache.tinkerpop.gremlin.structure.Edge
import org.json4s.{ DefaultFormats, JsonAST }
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{ compact, render }
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object Util extends LazyLogging {

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

    arrayVertexReformated.foreach(v => logger.debug(v.toString))
    arrayVertexReformated
  }

  def extractValue(map: Map[Any, List[Any]], nameValue: String): (Any, PropertyType) = {
    val value = map.get(nameValue) match {
      case Some(x) => x.head.toString
      case None => throw new IllegalArgumentException("response is null")
    }
    if (isAllDigits(value)) (value.toLong, PropertyType.Long) else (value, PropertyType.String)
  }

  def isAllDigits(x: String): Boolean = x forall Character.isDigit

  /**
    * Converts a Map<<String>, List<String>> into a List<KeyValues<String>>.
    *
    * @param theMap the map containing the data.
    * @param keys   array of <Key> contained in the map.
    * @return a List<KeyValues<String>>.
    */
  def recompose(theMap: Map[Any, List[Any]], keys: List[String]): List[ElementProperty] = {
    val resWithId = theMap map {
      x =>
        val pos = keys.indexOf(x._1)
        if (pos == -1) throw KeyNotInList(s"key ${x._1.asInstanceOf[String]} is not contained in the list of keys")
        val keyName = keys(pos)
        val value: (Any, PropertyType) = extractValue(theMap, keyName)
        value._2 match {
          case PropertyType.String => ElementProperty(KeyValue(new Key[Any](keyName), value._1.asInstanceOf[String]), PropertyType.String)
          case PropertyType.Long => ElementProperty(KeyValue(new Key[Any](keyName), value._1.asInstanceOf[Long]), PropertyType.Long)
        }
    }
    resWithId.toList
  }

  /**
    *
    * @param gc    The gremlin connector.
    * @param vFrom vertex from where the edge goes.
    * @param vTo   vertex to where the edge goes.
    * @param size  Number of expected edges connecting the vertexes (default: 1).
    * @return The edge.
    */
  def getEdge(implicit gc: GremlinConnector, vFrom: VertexDatabase, vTo: VertexDatabase, size: Int = 1): List[Edge] = {
    val edgeList = gc.g.V(vFrom.vertex).outE().as("e").inV().is(vTo.vertex).select("e").l() //filter(_.inV().is(vTo.vertex)).toList()
    edgeList match {
      case x: List[Edge] =>
        if (x.size != size) throw NumberOfEdgesNotCorrect(s"The required number of edges linked the two vertices is not met: ${x.size}")
        size match {
          case 0 => null
          case _ => x
        }
      case _ => null
    }
  }

  def getEdgeProperties(implicit gc: GremlinConnector, edge: Edge): Map[Any, List[String]] = {
    val edgePropertiesAsJava = gc.g.E(edge).valueMap().toList().head.asScala.toMap.asInstanceOf[Map[Any, Any]]
    edgePropertiesAsJava map { x => x._1 -> List(x._2.asInstanceOf[String]) }
  }

  def kvToJson(keyValue: ElementProperty): (String, String) = keyValue.keyName -> keyValue.value.toString

  def relationStrategyJson(relation: RelationServer, strat: String): String = compact(render("RelationStrategy" -> ("type" -> strat) ~ ("relation" -> relation.toJson)))

}

