package com.ubirch.discovery.consumer

import com.typesafe.config.Config
import com.ubirch.discovery.models._
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.process.{ Executor, Storer }
import com.ubirch.discovery.services.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.util.{ ErrorsHandler, Timer }
import com.ubirch.discovery.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.discovery.Lifecycle
import com.ubirch.kafka.express.ExpressKafka
import gremlin.scala.Vertex
import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

trait DiscoveryApp {

  def letsProcess(crs: Vector[ConsumerRecord[String, String]]): Unit

  def parseRelations(data: String): Seq[Relation]

  def store(relations: Seq[Relation]): List[(DumbRelation, Try[Any])]

}

abstract class AbstractDiscoveryService(storer: Storer, config: Config, lifecycle: Lifecycle) extends DiscoveryApp with ExpressKafka[String, String, Unit] {

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180

  override val producerBootstrapServers: String = config.getString("kafkaApi.kafkaProducer.bootstrapServers")
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[String] = new StringSerializer
  override val consumerTopics: Set[String] = config.getString("kafkaApi.kafkaProducer.topic").split(", ").toSet

  val producerErrorTopic: String = config.getString("kafkaApi.kafkaConsumer.errorTopic")

  override val consumerBootstrapServers: String = config.getString("kafkaApi.kafkaConsumer.bootstrapServers")
  override val consumerGroupId: String = config.getString("kafkaApi.kafkaConsumer.groupId")
  override val consumerMaxPollRecords: Int = config.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")
  override val consumerGracefulTimeout: Int = config.getInt("kafkaApi.kafkaConsumer.gracefulTimeout")
  override val lingerMs: Int = config.getInt("kafkaApi.kafkaProducer.lingerMS")
  override val metricsSubNamespace: String = config.getString("kafkaApi.metrics.prometheus.namespace")
  override val consumerReconnectBackoffMsConfig: Long = config.getLong("kafkaApi.kafkaConsumer.reconnectBackoffMsConfig")
  override val consumerReconnectBackoffMaxMsConfig: Long = config.getLong("kafkaApi.kafkaConsumer.reconnectBackoffMaxMsConfig")
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  override def consumerFetchMaxBytesConfig: Int = 52428800
  override def consumerMaxPartitionFetchBytesConfig: Int = 10485760

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
  private val errorCounter: Counter = new DefaultConsumerRecordsErrorCounter
  private val storeCounter: Counter = new DefaultConsumerRecordsSuccessCounter

  val maxParallelConnection: Int = config.getInt("kafkaApi.gremlinConf.maxParallelConnection") // PUT AT 1 FOR TESTS

  val batchSize: Int = config.getInt("kafkaApi.batchSize")

  lazy val flush: Boolean = config.getBoolean("flush")

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

  private def stopIfEmptyMessage(data: String): Unit = {
    data match {
      case "" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case "[]" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case _ =>
    }
  }

  def store(relations: Seq[Relation]): List[(DumbRelation, Try[Any])] = {

    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    val timedResult = Timer.time({

      val hashMapVertices: Map[VertexCore, Vertex] = preprocess(relations)

      def getVertexFromHMap(vertexCore: VertexCore): Vertex = {
        hashMapVertices.get(vertexCore) match {
          case Some(vDb) => vDb
          case None =>
            logger.info(s"getVertexFromHMap vertex not found in HMAP ${vertexCore.toString}")
            storer.getUpdateOrCreateSingle(vertexCore)
        }
      }

      logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.size}")
      val relationsAsRelationServer: Seq[DumbRelation] = relations.map(r => DumbRelation(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge))

      val executor = new Executor[DumbRelation, Any](objects = relationsAsRelationServer, f = storer.createRelation, processSize = maxParallelConnection, customResultFunction = Some(() => AbstractDiscoveryService.this.increasePrometheusRelationCount()))
      executor.startProcessing()
      executor.latch.await(100, java.util.concurrent.TimeUnit.SECONDS)
      executor.getResults

    })
    //res.logTimeTakenJson(s"process_relations" -> List(("size" -> relations.size) ~ ("value" -> relations.map { r => r.toJson }.toList)), 10000, warnOnly = false)

    timedResult.result match {
      case Success(success) =>
        // print totalNumberRel,numberVertice,sizePreprocess,timeTakenProcessAll,timeTakenIndividuallyRelation
        //val verticesNumber = Store.getAllVerticeFromRelations(relations).toList.size
        logger.info(s"processed {${relations.size},${timedResult.elapsed.toDouble / relations.size.toDouble},${timedResult.elapsed}}")
        success
      case Failure(exception) =>
        logger.error("Error storing relations, out of executor", exception)
        throw StoreException("Error storing relations, out of executor", exception)
    }
  }

  /**
    * The preprocess class take a list of relations (relation = 2 vertices linked by an edge) and create a hashmap of
    * all individual vertices. All the vertices will be added on JG through the executor.
    * @param relations The list of relations that contains the vertices
    * @return a hashmap corresponding, on the key, to the individual vertices, and on the value, to the janusgraph vertex
    */
  def preprocess(relations: Seq[Relation]): Map[VertexCore, Vertex] = {
    // 1: flatten relations to get the vertices
    val distinctVertices: List[VertexCore] = relations.flatMap(r => List(r.vFrom, r.vTo)).distinct.toList

    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](objects = distinctVertices.grouped(batchSize).toSeq, f = storer.getUpdateOrCreateVertices(_), processSize = maxParallelConnection)
    executor.startProcessing()
    executor.latch.await()
    val j = executor.getResultsNoTry
    j.flatMap(r => r._2).toMap

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

  lifecycle.addStopHook { () =>
    logger.info("Shutting down kafka")
    Future.successful(consumption.shutdown(consumerGracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
  }

}

@Singleton
class DefaultDiscoveryService @Inject() (storer: Storer, config: Config, lifecycle: Lifecycle)(implicit val ec: ExecutionContext) extends AbstractDiscoveryService(storer, config, lifecycle)
