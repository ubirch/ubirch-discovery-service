package com.ubirch.discovery.util

import com.ubirch.discovery.util.Exceptions.{ ParsingException, StoreException }
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{ compact, render }

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
  def generateException(exception: Throwable, relation: String = ""): String = {
    val errorsStruct = exception match {
      case e: ParsingException => ParsingError(e.message + " relation: " + relation)
      case e: StoreException => StorageError(e.message + " relation: " + relation)
    }
    toJson(errorsStruct)
  }

  def toJson(error: ErrorsStruct): String = {
    val json = ("error_type" -> error.errorName) ~ ("message" -> error.messageError)
    compact(render(json))
  }
}
