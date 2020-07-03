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

  def kvToJson(keyValue: ElementProperty): (String, String) = keyValue.keyName -> keyValue.value.toString
}
