package com.ubirch.discovery.kafka.util

object Exceptions {

  case class ParsingException(message: String) extends Exception(message)

  case class StoreException(message: String, error: Throwable = null) extends Exception(message, error)

}
