package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.kafka.models.{ AddV, Store }
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.language.postfixOps
import scala.util.Try

trait DefaultExpressDiscoveryApp extends ExpressKafkaApp[String, String] {

  override val producerBootstrapServers: String = conf.getString("kafkaApi.kafkaProducer.bootstrapServers")

  override val keySerializer: serialization.Serializer[String] = new StringSerializer

  override val valueSerializer: serialization.Serializer[String] = new StringSerializer

  override val consumerTopics: Set[String] = conf.getString("kafkaApi.kafkaProducer.topic").split(", ").toSet

  val producerErrorTopic: String = conf.getString("kafkaApi.kafkaConsumer.errorTopic")

  override val consumerBootstrapServers: String = conf.getString("kafkaApi.kafkaConsumer.bootstrapServers")

  override val consumerGroupId: String = conf.getString("kafkaApi.kafkaConsumer.groupId")

  override val consumerMaxPollRecords: Int = conf.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")

  override val consumerGracefulTimeout: Int = conf.getInt("kafkaApi.kafkaConsumer.gracefulTimeout")

  override val keyDeserializer: Deserializer[String] = new StringDeserializer

  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  override def process(consumerRecords: Vector[ConsumerRecord[String, String]]): Unit = {
    consumerRecords.foreach { cr =>

      logger.debug("Received value: " + cr.value())

      Try(parseRelations(cr.value())).map(store).recover {
        case e: ParsingException =>
          send(producerErrorTopic, ErrorsHandler.generateException(e))
          logger.error(ErrorsHandler.generateException(e))
        case e: StoreException =>
          send(producerErrorTopic, ErrorsHandler.generateException(e))
          logger.error(ErrorsHandler.generateException(e))
      }

    }
  }

  def parseRelations(data: String): Seq[AddV] = {
    implicit val formats: DefaultFormats = DefaultFormats
    try {
      jackson.parseJson(data).extract[Seq[AddV]]
    } catch {
      case e: Exception =>
        throw ParsingException(s"Error parsing data [${e.getMessage}]")
    }
  }

  def store(data: Seq[AddV]): Boolean = {
    try {
      data.foreach(Store.addV)
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

}
