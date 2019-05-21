package com.ubirch.discovery.core

import gremlin.scala.{Key, KeyValue}
import org.apache.tinkerpop.gremlin.structure.Edge
import org.scalatest.{FeatureSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object Util extends FeatureSpec with Matchers {

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Get the value associated to a map<<String>, List<T>> based on the parameter.
    *
    * @param map       The map.
    * @param nameValue the name on which we want the value.
    * @tparam T Type of value we're looking for.
    * @return value of type T.
    */
  def extractValue[T](map: Map[Any, List[Any]], nameValue: String): T = {
    map.get(nameValue) match {
      case Some(x) => x.head.asInstanceOf[T]
      case None => throw new IllegalArgumentException("response is null")
    }
  }

  /**
    * Converts a Map<<String>, List<String>> into a List<KeyValues<String>>.
    *
    * @param theMap the map containing the data.
    * @param keys   array of <Key> contained in the map.
    * @return a List<KeyValues<String>>.
    */
  def recompose(theMap: Map[Any, List[Any]], keys: Array[Key[String]]): List[KeyValue[String]] = {
    val resWithId = theMap map {
      x =>
        val pos = keys.indexOf(Key[String](x._1.asInstanceOf[String]))
        keys(pos) -> KeyValue(keys(pos), extractValue(theMap, keys(pos).name))
    }
    if (keys.indexOf(Key[String]("IdAssigned")) != -1) {
      (resWithId - Key[String]("IdAssigned")).values.toList
    } else {
      resWithId.values.toList
    }
  }

  def getEdge(implicit gc: GremlinConnector, idFrom: String, idTo: String, key: Key[String], size: Int = 1): Edge = {
    val edgeList = gc.g.V().has(key, idFrom).outE().as("e").inV().has(key, idTo).select("e").toList()
    edgeList match {
      case x: List[Edge] =>
        x.size shouldBe size
        size match {
          case 0 => null
          case _ => x.head
        }
      case _ => null
    }
  }

  def getEdgeProperties(implicit gc: GremlinConnector, edge: Edge): Map[Any, List[String]] = {
    val res = gc.g.E(edge).valueMap().toList().head.asScala.toMap.asInstanceOf[Map[Any, Any]]
    res map { x => x._1 -> List(x._2.asInstanceOf[String]) }
  }

}
