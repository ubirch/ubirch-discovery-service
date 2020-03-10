package com.ubirch.discovery.kafka.consumer

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.core.structure.Relation
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.{Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter}
import com.ubirch.discovery.kafka.models.{RelationKafka, Store}
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ParsingException, StoreException}
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.jackson.Serialization

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait DefaultExpressDiscoveryApp extends ExpressKafkaApp[String, String, Unit] {

  override val producerBootstrapServers: String = conf.getString("kafkaApi.kafkaProducer.bootstrapServers")

  override val keySerializer: serialization.Serializer[String] = new StringSerializer

  override val valueSerializer: serialization.Serializer[String] = new StringSerializer

  override val consumerTopics: Set[String] = conf.getString("kafkaApi.kafkaProducer.topic").split(", ").toSet

  val producerErrorTopic: String = conf.getString("kafkaApi.kafkaConsumer.errorTopic")

  override val consumerBootstrapServers: String = conf.getString("kafkaApi.kafkaConsumer.bootstrapServers")

  override val consumerGroupId: String = conf.getString("kafkaApi.kafkaConsumer.groupId")

  override val consumerMaxPollRecords: Int = conf.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")

  override val consumerGracefulTimeout: Int = conf.getInt("kafkaApi.kafkaConsumer.gracefulTimeout")

  override val lingerMs: Int = conf.getInt("kafkaApi.kafkaProducer.lingerMS")

  override val metricsSubNamespace: String = conf.getString("kafkaApi.metrics.prometheus.namespace")

  override val consumerReconnectBackoffMsConfig: Long = conf.getLong("kafkaApi.kafkaConsumer.reconnectBackoffMsConfig")

  override val consumerReconnectBackoffMaxMsConfig: Long = conf.getLong("kafkaApi.kafkaConsumer.reconnectBackoffMaxMsConfig")

  override val keyDeserializer: Deserializer[String] = new StringDeserializer

  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  private val errorCounter: Counter = new DefaultConsumerRecordsErrorCounter

  private val storeCounter: Counter = new DefaultConsumerRecordsSuccessCounter

  case class RelationWrapper(tpe: String, data: RelationKafka)

  override def process: Process = Process { crs =>
    crs.foreach { cr =>

      logger.debug("Received value: " + cr.value())
      storeCounter.counter.labels("ReceivedMessage").inc()

      Try(parseRelations(cr.value()))
        .recover {
          case exception: ParsingException =>
            errorCounter.counter.labels("ParsingException").inc()
            send(producerErrorTopic, ErrorsHandler.generateException(exception, cr.value()))
            logger.error(ErrorsHandler.generateException(exception, cr.value()))
            Nil
        }
        .filter(_.nonEmpty)
        .map { x =>
          if (checkIfAllVertexAreTheSame(x)) {
            Try(storeCache(x)) recover {
              case e: StoreException =>
                errorCounter.counter.labels("StoreException").inc()
                send(producerErrorTopic, ErrorsHandler.generateException(e, cr.value()))
                logger.error(ErrorsHandler.generateException(e, cr.value()))
            }
          } else {
            Try(store(x)) recover {
              case e: StoreException =>
                errorCounter.counter.labels("StoreException").inc()
                send(producerErrorTopic, ErrorsHandler.generateException(e, cr.value()))
                logger.error(ErrorsHandler.generateException(e, cr.value()))
            }
          }
        }

    }

  }

  def checkIfAllVertexAreTheSame(relations: Seq[Relation]): Boolean = {
    if (relations.size <= 3) false else
      relations.forall(r => relations.head.vFrom.equals(r.vFrom))
  }

  def parseRelations(data: String): Seq[Relation] = {

    implicit val formats: DefaultFormats = DefaultFormats
    // val messageType = (jackson.parseJson(data) \ "type").extract[String]
    stopIfEmptyMessage(data)
    try {
      val relationAsInternalStruct = jackson.parseJson(data).extract[Seq[RelationKafka]]
      relationAsInternalStruct map { r => r.toCoreRelation }
    } catch {
      case e: Exception => throw ParsingException(s"""Error parsing data [${e.getMessage}]""")
    }
  }

  def stopIfEmptyMessage(data: String): Unit = {
    data match {
      case "" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case "[]" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case _ =>
    }
  }

  def store(data: Seq[Relation]): Boolean = {
    try {
      val timer = new Timer()

      // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
      // that could create a ConnectionTimeOutException.
      if (data.size > 3) {
        val dataPartition = data.grouped(16).toList

        dataPartition foreach { batchOfAddV =>
          //          logger.info("sleeping")
          //          Thread.sleep(310000)
          //          logger.info("Finished the siesta")
          val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
          batchOfAddV.foreach { x =>
            logger.debug(s"relationship: ${x.toString}")
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
      } else {
        data.foreach { x =>
          /*          logger.info("sleeping")
          Thread.sleep(310000)
          logger.info("Finished the siesta")*/
          logger.debug(s"relationship: ${x.toString}")
          Store.addV(x)
          storeCounter.counter.labels("RelationshipStoredSuccessfully").inc()
        }
      }

      timer.finish(s"process message MSG: ${Serialization.write(data)} of size ${data.size} ")
      storeCounter.counter.labels("MessageStoredSuccessfully").inc()
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + s"""${e.getMessage}""")
        throw StoreException("Error storing graph: " + s"""${e.getMessage}""")
    }
  }

  def storeCache(data: Seq[Relation]): Boolean = {
    try {
      val timer = new Timer()
      val vertexCached = Store.vertexToCache(data.head.vFrom)

      // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
      // that could create a ConnectionTimeOutException.
      val dataPartition = data.grouped(16).toList

      dataPartition foreach { batchOfAddV =>
        /*        logger.info("sleeping")
        Thread.sleep(310000)
        logger.info("Finished the siesta")*/
        logger.debug(s"STARTED sending a batch of ${batchOfAddV.size} asynchronously")
        val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
        batchOfAddV.foreach { x =>
          logger.debug(s"relationship: ${x.toString}")
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

      storeCounter.counter.labels("MessageStoredSuccessfully").inc(data.length)
      timer.finish(s"process CACHED message MSG: ${Serialization.write(data)} of size ${data.size} ")
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

}

