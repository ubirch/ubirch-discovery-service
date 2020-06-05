package com.ubirch.discovery.util

import com.ubirch.discovery.models.{ ElementProperty, PropertyType }
import com.ubirch.discovery.models.PropertyType.PropertyType
import com.ubirch.discovery.util.Exceptions.KeyNotInList
import gremlin.scala.{ Key, KeyValue }

object Util {

  def convertProp(name: String, value: Any): ElementProperty = {
    val newValue = castToCorrectType(value)
    ElementProperty(KeyValue[Any](Key[Any](name), newValue._1), newValue._2)
  }

  def castToCorrectType(value: Any): (Any, PropertyType.Value) = {
    val vAsString = value.toString
    if (isAllDigits(vAsString)) (vAsString.toLong, PropertyType.Long) else (vAsString, PropertyType.String)
  }

  def isAllDigits(x: String): Boolean = x forall Character.isDigit

  def extractValue(map: Map[Any, List[Any]], nameValue: String): (Any, PropertyType) = {
    val value = map.get(nameValue) match {
      case Some(x) => x.head.toString
      case None => throw new IllegalArgumentException("response is null")
    }
    if (isAllDigits(value)) (value.toLong, PropertyType.Long) else (value, PropertyType.String)
  }

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

  def kvToJson(keyValue: ElementProperty): (String, String) = keyValue.keyName -> keyValue.value.toString
}
