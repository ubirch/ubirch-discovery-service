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
import org.json4s.JsonDSL._

import scala.collection.immutable
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
            logger.debug("Storing with cache")
            Try(storeCache(x)) recover {
              case e: StoreException =>
                errorCounter.counter.labels("StoreException").inc()
                send(producerErrorTopic, ErrorsHandler.generateException(e, cr.value()))
                logger.error(ErrorsHandler.generateException(e, cr.value()))
            }
          } else {
            logger.debug("Storing without cache")
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
      Timer.time({
        if (data.size > 3) {
          // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
          // that could create a ConnectionTimeOutException.
          val relationsPartition: immutable.Seq[Seq[Relation]] = data.grouped(16).toList

          relationsPartition foreach { batchOfRelations =>
            val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
            Timer.time {
              Store.addVerticesPresentMultipleTimes(batchOfRelations.toList)
            }.logTimeTaken("add vertices present multiple times")
            batchOfRelations.foreach { relation =>

              val process = Future(Store.addRealation(relation))
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
          Timer.time {
            Store.addVerticesPresentMultipleTimes(data.toList)
          }.logTimeTaken("add vertices present multiple times")
          data.foreach { x =>

            Store.addRealation(x)
            storeCounter.counter.labels("RelationshipStoredSuccessfully").inc()
          }
        }
      }).logTimeTakenJson(s"process_relations" -> List(("size" -> data.size) ~ ("value" -> data.map { r => r.toJson }.toList)))

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
      Timer.time({
        val vertexCached = Store.vertexToCache(data.head.vFrom)

        // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
        // that could create a ConnectionTimeOutException.
        val dataPartition = data.grouped(16).toList

        dataPartition foreach { batchOfRelations =>
          logger.debug(s"STARTED sending a batch of ${batchOfRelations.size} asynchronously")
          val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
          Store.addVerticesPresentMultipleTimes(batchOfRelations.toList)

          batchOfRelations.foreach { relation =>
            val process = Future(Store.addVCached(relation, vertexCached))
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
          logger.debug(s"FINISHED sending a batch of ${batchOfRelations.size} asynchronously")

        }
      }).logTimeTaken(s"processed CACHED message MSG of size ${data.size} : ${data.map(d => d.toString).mkString(", ")} ")

      storeCounter.counter.labels("MessageStoredSuccessfully").inc(data.length)
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

}

