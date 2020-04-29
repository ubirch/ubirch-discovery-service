package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.{ DumbRelation, Relation, VertexCore }
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.{ Helpers, Timer }
import com.ubirch.discovery.kafka.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.kafka.models.{ Executor, KafkaElements, RelationKafka, Store }
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.express.ExpressKafkaApp
import com.ubirch.kafka.producer.ProducerRunner
import com.ubirch.niomon.healthcheck.{ Checks, HealthCheckServer }
import com.ubirch.niomon.healthcheck.HealthCheckServer.CheckerFn
import gremlin.scala.Vertex
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord }
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
import scala.concurrent.ExecutionContext
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

  override def consumerFetchMaxBytesConfig: Int = 52428800

  override def consumerMaxPartitionFetchBytesConfig: Int = 10485760

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  private val errorCounter: Counter = new DefaultConsumerRecordsErrorCounter

  private val storeCounter: Counter = new DefaultConsumerRecordsSuccessCounter

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

  // TODO: change this once going back to normal behaviour
  val maxParallelConnection: Int = 1 //conf.getInt("kafkaApi.gremlinConf.maxParallelConnection") // FOR TESTS

  lazy val flush: Boolean = conf.getBoolean("flush")

  //  val healthCheckServer = new HealthCheckServer(Map(), Map())
  //  initHealthChecks()

  override val process: Process = Process { crs => letsProcess(crs) }

  /*
  * Logic of process put as a separate method in order to test more easily
    */
  def letsProcess(crs: Vector[ConsumerRecord[String, String]]): Unit = {
    if (!flush) {
      try {

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

      } catch {
        case e: Exception =>
          //TODO WHAT IS OK TO LET GO
          logger.error("Error processing: ", e)
      }
    } else {
      logger.debug("flushing")
    }
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

  def store(relations: Seq[Relation]): List[(DumbRelation, Try[Any])] = {

    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    val preprocessBatchSize = 1 + scala.util.Random.nextInt(100)
    val res = Timer.time({

      val hashMapVertices: Map[VertexCore, Vertex] = preprocess(relations, preprocessBatchSize)

      def getVertexFromHMap(vertexCore: VertexCore) = {
        hashMapVertices.get(vertexCore) match {
          case Some(vDb) => vDb
          case None =>
            logger.info(s"getVertexFromHMap vertex not found in HMAP ${vertexCore.toString}")
            Helpers.getUpdateOrCreate(vertexCore)
        }
      }

      logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.size}")
      val relationsAsRelationServer: Seq[DumbRelation] = relations.map(r => DumbRelation(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge))

      val executor = new Executor[DumbRelation, Any](objects = relationsAsRelationServer, f = Helpers.createRelation(_), processSize = maxParallelConnection, customResultFunction = Some(() => DefaultExpressDiscoveryApp.this.increasePrometheusRelationCount()))
      executor.startProcessing()
      executor.latch.await()
      executor.getResults

    })
    //res.logTimeTakenJson(s"process_relations" -> List(("size" -> relations.size) ~ ("value" -> relations.map { r => r.toJson }.toList)), 10000, warnOnly = false)

    res.result match {
      case Success(success) =>
        // print totalNumberRel,numberVertice,sizePreprocess,timeTakenProcessAll,timeTakenIndividuallyRelation
        val verticesNumber = Store.getAllVerticeFromRelations(relations).toList.size
        logger.info(s"processed {${relations.size},$verticesNumber,$preprocessBatchSize,${res.elapsed.toDouble / relations.size.toDouble}}")
        success
      case Failure(exception) =>
        logger.error("Error storing relations, out of executor", exception)
        throw StoreException("Error storing relations, out of executor", exception)
    }
  }

  def preprocess(relations: Seq[Relation], batchSize: Int): Map[VertexCore, Vertex] = {
    // 1: flatten relations to get the vertices
    val t0 = System.currentTimeMillis()
    val vertices: List[VertexCore] = Store.getAllVerticeFromRelations(relations).toList
    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    val verticesGroups: Seq[List[VertexCore]] = vertices.grouped(batchSize).toSeq

    val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](objects = verticesGroups, f = Helpers.getUpdateOrCreateMultiple(_), processSize = maxParallelConnection, customResultFunction = Some(() => DefaultExpressDiscoveryApp.this.increasePrometheusRelationCount()))
    executor.startProcessing()
    executor.latch.await()
    val j = executor.getResultsNoTry
    val theRes: Map[VertexCore, Vertex] = j.flatMap(r => r._2).toMap
    val t1 = System.currentTimeMillis()

    //val res: Map[VertexCore, Vertex] = verticesGroups.map{vs => Helpers.getUpdateOrCreateMultiple(vs.toList)}.toList.flatten.toMap
    logger.info(s"preprocess of ${vertices.size} done in ${t1 - t0} ms => ${(t1 - t0).toDouble / vertices.size.toDouble} ms/vertex")
    theRes
  }

  def recoverStoreRelationIfNeeded(relationAndResult: (DumbRelation, Try[Any])): Try[Any] = {
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

  //  def initHealthChecks(): Unit = {
  //    if (!conf.getBoolean("kafkaApi.healthcheck.enabled")) return
  //
  //    def addChecksForProducerRunner(name: String, producerRunner: ProducerRunner[_, _]): Unit = {
  //      healthCheckServer.setReadinessCheck(name) { implicit ec =>
  //        producerRunner.getProducerAsOpt match {
  //          case Some(producer) => Checks.kafka(name, producer, connectionCountMustBeNonZero = false)._2(ec)
  //          case None => Checks.notInitialized(name)._2(ec)
  //        }
  //      }
  //    }
  //
  //    def addChecksForConsumerRunner(name: String, consumerRunner: ConsumerRunner[_, _]): Unit = {
  //
  //      healthCheckServer.setReadinessCheck(name) { implicit ec =>
  //        Checks.notInitialized(name)._2(ec)
  //      }
  //    }
  //
  //    def addReachabilityCheckForProducerRunner(checkName: String, producerRunner: ProducerRunner[_, _]): Unit = {
  //      def checkReachabilityForRunner(producerRunner: ProducerRunner[_, _]): CheckerFn = { ec: ExecutionContext =>
  //        producerRunner.getProducerAsOpt match {
  //          case Some(producer) => Checks.kafkaNodesReachable(producer)._2(ec)
  //          case None =>
  //            Checks.notInitialized(checkName)._2(ec)
  //        }
  //      }
  //
  //      healthCheckServer.setReadinessCheck(checkName)(checkReachabilityForRunner(producerRunner))
  //      healthCheckServer.setLivenessCheck(checkName)(checkReachabilityForRunner(producerRunner))
  //    }
  //
  //    healthCheckServer.setLivenessCheck(Checks.process())
  //    healthCheckServer.setReadinessCheck(Checks.process())
  //
  //    addChecksForProducerRunner("express-kafka-producer", production)
  //    addChecksForConsumerRunner("express-kafka-consumer", consumption)
  //    addReachabilityCheckForProducerRunner("kafka-nodes-reachable", production)
  //
  //    healthCheckServer.run(conf.getInt("kafkaApi.healthcheck.port"))
  //  }

}

