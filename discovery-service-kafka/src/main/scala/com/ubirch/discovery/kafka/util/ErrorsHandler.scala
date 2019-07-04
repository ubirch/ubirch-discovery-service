package com.ubirch.discovery.kafka.util

import com.ubirch.discovery.kafka.util.Exceptions.{ParsingException, StoreException}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

trait ErrorsStruct {
  val errorName: String
  val messageError: String
}

case class ParsingError(message: String) extends ErrorsStruct {
  override val errorName = "Parsing Error"
  override val messageError: String = message
}

case class StorageError(message: String) extends ErrorsStruct {
  override val errorName = "Storage Error"
  override val messageError: String = message
}

object ErrorsHandler {
  def generateException(exception: Exception): String = {
    val errorsStruct = exception match {
      case e: ParsingException => ParsingError(e.message)
      case e: StoreException => StorageError(e.message)
    }
    toJson(errorsStruct)
  }

  def toJson(error: ErrorsStruct): String = {
    val json = ("Error type" -> error.errorName) ~ ("Message" -> error.messageError)
    compact(render(json))
  }
}
