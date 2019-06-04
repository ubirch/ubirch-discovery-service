package com.ubirch.discovery.kafka.consumer

import java.util.UUID
import java.util.concurrent.CountDownLatch

import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.discovery.kafka.models.{ AddV, Store }
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.consumer._
import org.apache.kafka.clients.consumer.{ ConsumerRecord, OffsetResetStrategy }
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try

object DefaultStringConsumer extends LazyLogging {

  val conf: Config = ConfigFactory.load("application.base.conf")
  val topics: Set[String] = conf.getStringList("kafkaApi.kafkaConsumer.topic").asScala.toSet

  val configs = Configs(
    bootstrapServers = conf.getString("kafkaApi.kafkaConsumer.bootstrapServers"),
    groupId = conf.getString("kafkaApi.kafkaConsumer.groupId"),
    enableAutoCommit = false,
    autoOffsetReset = OffsetResetStrategy.EARLIEST,
    maxPollRecords = conf.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")
  )

  val myController: ConsumerRecordsController[String, String] = new ConsumerRecordsController[String, String] {

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

    override type A = ProcessResult[String, String]

    override def process(consumerRecord: Vector[ConsumerRecord[String, String]]): Future[ProcessResult[String, String]] = {
      consumerRecord.foreach { cr =>
        //TODO: WE NEED TO HANDLE ERRORS SO WE CAN CONTINUE CONSUMING AFTER ERRORS
        logger.debug("Received value: " + cr.value())

        Try(parseRelations(cr.value())).map(store).recover {
          case e: ParsingException =>
            logger.error("Error Parsing Value [{}]", e.getMessage)
          case e: StoreException =>
            logger.error("Error Storing Relation [{}]", e.getMessage)
        }

      }

      Future.successful(new ProcessResult[String, String] {
        override val id: UUID = UUID.randomUUID()
        override val consumerRecords: Vector[ConsumerRecord[String, String]] = consumerRecord
      })
    }
  }

  lazy val consumerConfigured: StringConsumer with WithMetrics[String, String] = {
    val consumerImp = new StringConsumer() with WithMetrics[String, String]
    consumerImp.setUseAutoCommit(false)
    consumerImp.setTopics(topics)
    consumerImp.setProps(configs)
    consumerImp.setKeyDeserializer(Some(new StringDeserializer()))
    consumerImp.setValueDeserializer(Some(new StringDeserializer()))
    consumerImp.setConsumerRecordsController(Some(myController))
    consumerImp
  }

  def start(): Unit = {
    consumerConfigured.start()
    val cd = new CountDownLatch(1)
    cd.await()
  }

  Lifecycle.get.addStopHook { () =>
    logger.info("Shutting down Consumer: " + consumerConfigured.getName)
    Future.successful(consumerConfigured.shutdown(2, java.util.concurrent.TimeUnit.SECONDS))
  }

}
