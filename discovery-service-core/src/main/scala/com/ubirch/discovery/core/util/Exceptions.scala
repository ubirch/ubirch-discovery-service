package com.ubirch.discovery.core.util

object Exceptions {

  /**
    * Thrown when an element wasn't successfully added to the database
    * @param message Message explaining the error
    */
  class ImportToGremlinException(message: String) extends Exception(message)

  case class KeyNotInList(message: String) extends ImportToGremlinException(message)

  case class NumberOfEdgesNotCorrect(msg: String) extends ImportToGremlinException(message = msg)

  case class PropertiesNotCorrect(msg: String) extends ImportToGremlinException("IdNotCorrect: " + msg)
}
