package com.ubirch.discovery.core.util

object Exceptions {

  /**
    * Thrown when an element wasn't successfully added to the database
    * @param message Message explaining the error
    */
  class ImportToGremlinException(message: String) extends Exception(message)

  case class KeyNotInList(message: String) extends ImportToGremlinException(message)

  case class NumberOfEdgesNotCorrect(message: String) extends ImportToGremlinException(message = message)

  case class PropertiesNotCorrect(message: String) extends ImportToGremlinException("PropertiesNotCorrect: " + message)
}
