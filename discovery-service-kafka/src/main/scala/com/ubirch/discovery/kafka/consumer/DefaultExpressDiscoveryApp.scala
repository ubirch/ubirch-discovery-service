package com.ubirch.discovery.kafka.consumer

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.kafka.metrics.{Counter, DefaultConsumerRecordsManagerCounter, DefaultMetricsLoggerCounter}
import com.ubirch.discovery.kafka.models.{AddV, Store}
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ParsingException, StoreException}
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.json4s._

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

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

  val asynchronousBatchSize: Int = conf.getInt("kafkaApi.gremlinConf.asynchronousBatchSize")

  val errorCounter: Counter = new DefaultConsumerRecordsManagerCounter
  val storeCounter: Counter = new DefaultMetricsLoggerCounter

  override def process(consumerRecords: Vector[ConsumerRecord[String, String]]): Unit = {
    consumerRecords.foreach { cr =>

      logger.debug("Received value: " + cr.value())
      storeCounter.counter.labels("ReceivedMessage").inc()

      Try(parseRelations(cr.value())).recover {
        case exception: ParsingException =>
          errorCounter.counter.labels("ParsingException").inc()
          send(producerErrorTopic, ErrorsHandler.generateException(exception))
          logger.error(ErrorsHandler.generateException(exception))
          Nil
      }.filter(_.nonEmpty).map { x =>
        if (checkIfAllVertexAreTheSame(x)) {
          Try(storeCache(x)) recover {
            case e: StoreException =>
              errorCounter.counter.labels("StoreException").inc()
              send(producerErrorTopic, ErrorsHandler.generateException(e))
              logger.error(ErrorsHandler.generateException(e))
          }
        } else {
          Try(store(x)) recover {
            case e: StoreException =>
              errorCounter.counter.labels("StoreException").inc()
              send(producerErrorTopic, ErrorsHandler.generateException(e))
              logger.error(ErrorsHandler.generateException(e))
          }
        }

      }

    }
  }

  def checkIfAllVertexAreTheSame(data: Seq[AddV]): Boolean = {
    if (data.size <= 3) false else
      data forall (data.head.v1.id == _.v1.id)
  }

  def parseRelations(data: String): Seq[AddV] = {
    implicit val formats: DefaultFormats = DefaultFormats
    data match {
      case "" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case "[]" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case _ =>
    }
    try {
      jackson.parseJson(data).extract[Seq[AddV]]
    } catch {
      case e: Exception =>
        throw ParsingException(s"Error parsing data [${e.getMessage}]")
    }
  }

  def store(data: Seq[AddV]): Boolean = {

    try {
      val t0 = System.nanoTime()

      // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
      // thus creating a ConnectionTimeOut exception
      val dataPartition = data.grouped(asynchronousBatchSize).toList

      dataPartition foreach { batchOfAddV =>
        val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
        import scala.concurrent.ExecutionContext.Implicits.global
        batchOfAddV.foreach { x =>
          val process = Future(Store.addV(x))
          storeCounter.counter.labels("RelationshipStoredSuccessfully").inc()
          processesOfFutures += process
        }

        val futureProcesses = Future.sequence(processesOfFutures)

        val latch = new CountDownLatch(1)
        futureProcesses.onComplete {
          case Success(_) =>
            latch.countDown()
          case Failure(e) =>
            logger.error("Something happened", e)
            latch.countDown()
        }
        latch.await()

      }

      val t1 = System.nanoTime()
      logger.debug(s"message of size ${data.size} processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
      storeCounter.counter.labels("MessageStoredSuccessfully").inc()
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

  def storeCache(data: Seq[AddV]): Boolean = {
    try {
      val t0 = System.nanoTime()
      val vertexCached = Store.vertexToCache(data.head.v1)

      // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
      // thus creating a ConnectionTimeOut exception
      val dataPartition = data.grouped(asynchronousBatchSize).toList

      dataPartition foreach { batchOfAddV =>
        logger.debug(s"STARTED sending a batch of ${batchOfAddV.size} asynchronously")
        val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
        import scala.concurrent.ExecutionContext.Implicits.global
        batchOfAddV.foreach { x =>
          val process = Future(Store.addVCached(x, vertexCached))
          storeCounter.counter.labels("RelationshipStoredSuccessfully").inc()
          processesOfFutures += process
        }

        val futureProcesses = Future.sequence(processesOfFutures)

        val latch = new CountDownLatch(1)
        futureProcesses.onComplete {
          case Success(_) =>
            latch.countDown()
          case Failure(e) =>
            logger.error("Something happened", e)
            latch.countDown()
        }
        latch.await()
        logger.debug(s"FINISHED sending a batch of ${batchOfAddV.size} asynchronously")

      }

      val t1 = System.nanoTime()
      logger.debug(s"CACHED - message of size ${data.size} processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
      storeCounter.counter.labels("MessageStoredSuccessfully").inc()
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

}
