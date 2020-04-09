package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.structure.Relation
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.{Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter}
import com.ubirch.discovery.kafka.models.{Executor, RelationKafka, Store}
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ParsingException, StoreException}
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.JsonDSL._

import scala.language.postfixOps
import scala.util.Try

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

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

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
        .map { relations =>
          if (checkIfAllVertexAreTheSame(relations)) {
            logger.debug("Storing with cache")
            storeCache(relations) foreach recoverStoreRelationIfNeeded
          } else {
            logger.debug("Storing without cache")
            store(relations) foreach recoverStoreRelationIfNeeded
          }
        }

    }

  }

  def recoverStoreRelationIfNeeded(relationAndResult: (Relation, Try[Unit])): Try[Unit] = {
    relationAndResult._2 recover {
      case e: StoreException =>
        errorCounter.counter.labels("StoreException").inc()
        logger.error(ErrorsHandler.generateException(e, relationAndResult._1.toString))
        send(producerErrorTopic, ErrorsHandler.generateException(e, relationAndResult._1.toString))
      case e: Exception =>
        errorCounter.counter.labels("Exception").inc()
        logger.error(ErrorsHandler.generateException(e, relationAndResult._1.toString))
        send(producerErrorTopic, ErrorsHandler.generateException(e, relationAndResult._1.toString))
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

  def store(relations: Seq[Relation]): List[(Relation, Try[Unit])] = {

    val res = Timer.time({
      Store.addVerticesPresentMultipleTimes(relations.toList)
      val executor = new Executor[Relation, Try[Unit]](relations, Store.addRelation, 16)
      executor.startProcessing()
      executor.latch.await()
      executor.getResultsNoTry
    })
    res.logTimeTakenJson(s"process_relations" -> List(("size" -> relations.size) ~ ("value" -> relations.map { r => r.toJson }.toList)))

    storeCounter.counter.labels("MessageStoredSuccessfully").inc()
    res.result.get

  }

  def storeCache(data: Seq[Relation]): List[(Relation, Try[Unit])] = {

    logger.info(s"number of relations: ${data.size}")

    val res = Timer.time({
      val vertexCached = Store.vertexToCache(data.head.vFrom)
      Store.addVerticesPresentMultipleTimes(data.toList)

      val executor = new Executor[Relation, Try[Unit]](data, Store.addRelationOneCached(_, vertexCached), 16)
      executor.startProcessing()

      executor.latch.await()

      executor.getResultsNoTry

    })
    res.logTimeTaken(s"processed CACHED message MSG of size ${data.size} : ${data.map(d => d.toString).mkString(", ")} ")

    storeCounter.counter.labels("MessageStoredSuccessfully").inc(data.length)
    res.result.get

  }

}

