package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.{ Relation, RelationServer, VertexCore, VertexDatabase }
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.kafka.models.{ Executor, RelationKafka, Store }
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._
import org.json4s.JsonDSL._

import scala.collection.immutable
import scala.collection.immutable.HashMap
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

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

  val maxParallelConnection: Int = conf.getInt("kafkaApi.gremlinConf.maxParallelConnection")

  override def process: Process = Process { crs =>

    val allRelations: immutable.Seq[Relation] = crs.flatMap {
      cr =>
        //logger.debug("Received value: " + cr.value())
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
          .get
    }
    logger.debug(s"Pooled ${crs.size} kafka messages containing ${allRelations.size} relations")
    store(allRelations) foreach recoverStoreRelationIfNeeded

    /*    val allRelations: immutable.Seq[(Relation, Relation => Try[Unit])] = crs.flatMap {
      cr =>
        logger.debug("Received value: " + cr.value())
        storeCounter.counter.labels("ReceivedMessage").inc()

        val relations = Try(parseRelations(cr.value()))
          .recover {
            case exception: ParsingException =>
              errorCounter.counter.labels("ParsingException").inc()
              send(producerErrorTopic, ErrorsHandler.generateException(exception, cr.value()))
              logger.error(ErrorsHandler.generateException(exception, cr.value()))
              Nil
          }
          .filter(_.nonEmpty)
          .get
        counter += relations.size
        if (checkIfAllVertexAreTheSame(relations)) {
          val vertexCached = Store.vertexToCache(relations.head.vFrom)
          relations map { r => (r, Store.addRelationOneCached(_, vertexCached)) }
        } else {
          relations map { r => (r, Store.addRelation(_)) }
        }
    }
    logger.debug(s"Should process $counter relations")

    store(allRelations) foreach recoverStoreRelationIfNeeded

    counter = 0*/
  }

  def checkIfAllVertexAreTheSame(relations: Seq[Relation]): Boolean = {
    if (relations.size <= 3) false else
      relations.forall(r => relations.head.vFrom.equals(r.vFrom))
  }

  def parseRelations(data: String): Seq[Relation] = {

    implicit val formats: DefaultFormats = DefaultFormats
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

  def store(relations: Seq[Relation]): List[(RelationServer, Try[Unit])] = {

    val res = Timer.time({

      val hashMapVertices: HashMap[VertexCore, VertexDatabase] = preprocess(relations)

      def getVertexFromHMap(vertexCore: VertexCore) = {
        hashMapVertices.get(vertexCore) match {
          case Some(vDb) => vDb
          case None => Store.addVertex(vertexCore)
        }
      }

      logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.size}")
      val relationsAsRelationServer: Seq[(RelationServer, RelationServer => Try[Unit])] = relations.map(r => (RelationServer(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge), Store.addRelationTwoCached(_)))

      val executor = new Executor[RelationServer, Try[Unit]](objects = relationsAsRelationServer, processSize = maxParallelConnection, customResultFunction = Some(() => DefaultExpressDiscoveryApp.this.increasePrometheusRelationCount()))
      executor.startProcessing()
      executor.latch.await()
      executor.getResultsNoTry

    })
    //res.logTimeTakenJson(s"process_relations" -> List(("size" -> relations.size) ~ ("value" -> relations.map { r => r.toJson }.toList)), 10000, warnOnly = false)

    res.result match {
      case Success(success) => success
      case Failure(exception) =>
        logger.error("Error storing relations, out of executor", exception)
        throw StoreException("Error storing relations, out of executor", exception)
    }
  }

  def preprocess(relations: Seq[Relation]) = {
    // 1: flatten relations to get the vertices
    val vertices = Store.getAllVerticeFromRelations(relations)

    // 2: create the hashMap [vertexCore, vertexDb]
    val executor = new Executor[VertexCore, VertexDatabase](objects = vertices.map { v => (v, Store.addVertex(_)) }, processSize = maxParallelConnection)
    executor.startProcessing()
    executor.latch.await()
    val executorRes: List[(VertexCore, VertexDatabase)] = executor.getResultsOnlySuccess
    HashMap(executorRes.map(vCoreAndVDb => vCoreAndVDb._1 -> vCoreAndVDb._2): _*)
  }

  def recoverStoreRelationIfNeeded(relationAndResult: (RelationServer, Try[Unit])): Try[Unit] = {
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

  def increasePrometheusRelationCount(): Unit = {
    storeCounter.counter.labels("RelationStoredSuccessfully").inc()
  }

}

