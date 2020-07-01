package com.ubirch.discovery

import java.util.Date

import org.apache.kafka.common.serialization.Deserializer
import org.json4s.{ DefaultFormats, Formats }
import org.json4s.ext.JavaTypesSerializers
import org.json4s.jackson.Serialization.read

/**
  * Represents the error that is eventually published to Kafka.
  *
  * After trying to process/store the relations, there's the possibility of getting an error.
  * This type is used to represent the error generated.
  *
  * @param message       represents a friendly error message.
  * @param exceptionName represents the name of the exception. E.g ParsingIntoEventLogException.
  * @param value         represent the event value.
  *                      It can be empty if a EmptyValueException was thrown or if the exception is not known
  *                      It can be a malformed event if a ParsingIntoEventLogException was thrown
  *                      It can be the well-formed event if a StoringIntoEventLogException was thrown
  * @param errorTime     represents the time when the error occurred
  * @param serviceName   represents the name of the service. By default, we use, error-service.
  */
case class DiscoveryError(
    message: String,
    exceptionName: String,
    value: String = "",
    errorTime: Option[Date] = Some(new java.util.Date()),
    serviceName: String = "discovery-service"
) {

  override def toString: String = {
    "{\"message\":\"" + message + "\"," +
      "\"exceptionName\":\"" + exceptionName + "\"," +
      "\"value\":\"" + value + "\"," +
      "\"errorTime\":\"" + errorTime + "\"," +
      "\"serviceName\":\"" + serviceName + "\"}"
  }
}

object DiscoveryErrorDeserializer extends Deserializer[DiscoveryError] {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  implicit val formats: Formats = DefaultFormats ++ JavaTypesSerializers.all

  override def close(): Unit = {}

  override def deserialize(_topic: String, data: Array[Byte]): DiscoveryError = {
    read[DiscoveryError](new String(data, "UTF8"))
  }
}

