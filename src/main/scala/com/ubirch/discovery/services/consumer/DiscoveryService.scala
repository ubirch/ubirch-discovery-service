package com.ubirch.discovery.services.consumer

import java.util.concurrent.{ ScheduledThreadPoolExecutor, TimeUnit }
import java.util.Calendar

import com.typesafe.config.Config
import com.ubirch.discovery.{ DiscoveryError, Lifecycle, Service }
import com.ubirch.discovery.models._
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.process.Executor
import com.ubirch.discovery.services.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.util.{ HealthUtil, Timer }
import com.ubirch.discovery.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.discovery.ConfPaths.{ ConsumerConfPaths, DiscoveryConfPath, ProducerConfPaths }
import com.ubirch.discovery.models.lock.Lock
import com.ubirch.discovery.services.connector.GremlinConnector
import com.ubirch.discovery.services.health.HealthChecks
import com.ubirch.kafka.express.ExpressKafka
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import gremlin.scala.Vertex
import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

trait DiscoveryApp {

  /**
    * General logic orchestrating everything
    * @param crs The consumer records that contain the relations to process
    */
  def letsProcess(crs: Vector[ConsumerRecord[String, String]]): Unit

  /**
    * The relations that will need to be parsed. Will be converted to a sequence of relation
    * @param data
    * @return A list of relation
    */
  def parseRelations(data: String): Option[Seq[Relation]]

  /**
    * Function that will write the relations in the graph.
    * @param relations Relations that will be stored in the graph
    * @return Status of the relations
    */
  def store(relations: Seq[Relation]): Option[List[(DumbRelation, Try[Any])]]

}

abstract class AbstractDiscoveryService(storer: Storer, config: Config, lifecycle: Lifecycle, locker: Lock) extends DiscoveryApp
  with ExpressKafka[String, String, Unit] with ConsumerConfPaths with ProducerConfPaths with DiscoveryConfPath {

  import AbstractDiscoveryService._

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180

  override val producerBootstrapServers: String = config.getString(PRODUCER_BOOTSTRAP_SERVERS)
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[String] = new StringSerializer
  override val consumerTopics: Set[String] = config.getString(CONSUMER_TOPICS).split(", ").toSet

  val producerErrorTopic: String = config.getString(PRODUCER_ERROR_TOPIC)

  override val consumerBootstrapServers: String = config.getString(CONSUMER_BOOTSTRAP_SERVERS)
  override val consumerGroupId: String = config.getString(CONSUMER_GROUP_ID)
  override val consumerMaxPollRecords: Int = config.getInt(CONSUMER_MAC_POOL_RECORDS)
  override val consumerGracefulTimeout: Int = config.getInt(CONSUMER_GRACEFUL_TIMEOUT)
  override val lingerMs: Int = config.getInt(PRODUCER_LINGER_MS)
  override val metricsSubNamespace: String = config.getString(METRICS_SUBNAMESPACE)
  override val consumerReconnectBackoffMsConfig: Long = config.getLong(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG)
  override val consumerReconnectBackoffMaxMsConfig: Long = config.getLong(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG)
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  override def consumerFetchMaxBytesConfig: Int = 52428800
  override def consumerMaxPartitionFetchBytesConfig: Int = 10485760

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
  private val errorCounter: Counter = new DefaultConsumerRecordsErrorCounter
  private val storeCounter: Counter = new DefaultConsumerRecordsSuccessCounter

  val maxParallelConnection: Int = config.getInt(GREMLIN_MAX_PARALLEL_CONN) // PUT AT 1 FOR TESTS

  val batchSize: Int = config.getInt(BATCH_SIZE)

  lazy val flush: Boolean = config.getBoolean(FLUSH)

  consumption.getConsumerRecordsController()

  override val process: Process = Process { crs => letsProcess(crs) }

  /*
  * Logic of process put as a separate method in order to test without invoking kafka
    */
  def letsProcess(crs: Vector[ConsumerRecord[String, String]]): Unit = {

    if (!locker.isConnected) {
      logger.warn("Redis connection could not be established, pausing for a while")
      throw NeedForPauseException("RedisConnectionError", "Not yet connected to redis")
    }

    if (!flush) {
      try {

        val allRelations: immutable.Seq[Relation] = crs.flatMap {
          cr =>
            storeCounter.counter.labels("ReceivedMessage").inc()
            parseRelations(cr.value()) match {
              case Some(relations) => relations
              case None => Nil
            }
        }
        if (allRelations.nonEmpty) {
          logger.debug(s"Pooled ${crs.size} kafka messages containing ${allRelations.size} relations")
          store(allRelations) match {
            case Some(value) => value foreach recoverStoreRelationIfNeeded
            case None =>
          }
        }

      } catch {
        case e: NeedForPauseException =>
          publishErrorMessage("Need for pause exception on: crs Vector[ConsumerRecord[String, String]]", crs.mkString(", ").replace("\"", "\\\""), e)
          throw e
        case e: Exception =>
          publishErrorMessage("General error when processing crs Vector[ConsumerRecord[String, String]]", crs.mkString(", ").replace("\"", "\\\""), e)
      }
    } else {
      logger.debug("flushing")
    }
  }

  def parseRelations(data: String): Option[Seq[Relation]] = {

    implicit val formats: DefaultFormats = DefaultFormats
    try {
      stopIfEmptyMessage(data)
      val relationAsInternalStruct = jackson.parseJson(data).extract[Seq[RelationKafka]]
      Some(relationAsInternalStruct map { r => r.toRelationCore })
    } catch {
      case e: Exception =>
        errorCounter.counter.labels("ParsingException").inc()
        publishErrorMessage("Error when parsing relations", data.replace("\"", "\\\""), e)
        None
    }
  }

  private def stopIfEmptyMessage(data: String): Unit = {
    data match {
      case "" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case "[]" => throw ParsingException(s"Error parsing data [received empty message: $data]")
      case _ =>
    }
  }

  def store(relations: Seq[Relation]): Option[List[(DumbRelation, Try[Any])]] = {

    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    preprocess(relations) match {
      case Some(hashMap) =>
        val hashMapVertices: VertexMap = hashMap

        def getVertexFromHMap(vertexCore: VertexCore): Vertex = {
          hashMapVertices.get(vertexCore) match {
            case Some(vDb) => vDb
            case None =>
              logger.info(s"getVertexFromHMap vertex not found in HMAP ${vertexCore.toString}. relations = ${relations.mkString(", ")}")
              storer.getUpdateOrCreateSingleConcrete(vertexCore)
          }
        }

        logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.size}")
        val relationsAsRelationServer: Seq[DumbRelation] = relations.map(r => DumbRelation(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge))

        val executor = new Executor[DumbRelation, Any](objects = relationsAsRelationServer, f = storer.createRelation, processSize = maxParallelConnection, customResultFunction = Some(_ => this.increasePrometheusRelationCount()))
        executor.startProcessing()
        executor.latch.await(100, java.util.concurrent.TimeUnit.SECONDS)
        Some(executor.getResults)
      case None => None
    }
  }

  /**
    * The preprocess class take a list of relations (relation = 2 vertices linked by an edge) and create a hashmap of
    * all individual vertices. All the vertices will be added on JG through the executor.
    * @param relations The list of relations that contains the vertices
    * @return a hashmap corresponding, on the key, to the individual vertices, and on the value, to the janusgraph vertex
    */
  def preprocess(relations: Seq[Relation]): Option[VertexMap] = {
    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
    // 1: flatten relations to get the vertices
    val distinctVertices: List[VertexCore] = getDistinctVertices(relations)
    validateVerticesAreCorrect(distinctVertices) match {
      case None =>
        val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](
          objects = distinctVertices.grouped(batchSize).toSeq,
          f = storer.getUpdateOrCreateVerticesConcrete(_),
          processSize = maxParallelConnection
        )
        executor.startProcessing()
        executor.latch.await() // that's blocking but it's because we have to wait for all the relations to be preprocessed before continuing
        val j = executor.getResultsNoTry
        Some(DefaultVertexMap(j.flatMap(r => r._2).toMap))
      case Some(value) =>
        publishErrorMessage(
          errorMessage = "At least one vertex does not contain an iterable (unique) property",
          value = value.mkString(", "),
          ex = new Exception()
        )
        None
    }

  }

  def recoverStoreRelationIfNeeded(relationAndResult: (DumbRelation, Try[Any])): Try[Any] = {
    relationAndResult._2 recover {
      case e: StoreException =>
        errorCounter.counter.labels("StoreException").inc()
        publishErrorMessage("Error storing relation", relationAndResult._1.toString, e)
      case e: Exception =>
        errorCounter.counter.labels("Exception").inc()
        publishErrorMessage("Error storing relation", relationAndResult._1.toString, e)
    }

  }

  def increasePrometheusRelationCount(): Unit = {
    storeCounter.counter.labels("RelationStoredSuccessfully").inc()
  }

  /**
    * A helper function for logging and publishing error messages.
    *
    * @param errorMessage A message informing about the error.
    * @param value        The value that caused the error.
    * @param ex           The exception being thrown.
    * @return
    */
  private def publishErrorMessage(
      errorMessage: String,
      value: String,
      ex: Throwable
  ): Future[Any] = {
    logger.error("SENDING TO KAFKA ERROR TOPIC: " + errorMessage + " value: " + value, ex.getMessage, ex)
    val producerRecordToSend = new ProducerRecord[String, String](
      producerErrorTopic,
      DiscoveryError(errorMessage, ex.getClass.getSimpleName, value).toString
    )
    send(producerRecordToSend)
      .recover { case e => logger.error(s"failure publishing to error topic: $errorMessage", e) }
  }

  protected def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = production.send(producerRecord)

  lifecycle.addStopHook { () =>
    logger.info("Shutting down kafka")
    Future.successful(consumption.shutdown(consumerGracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
  }

  // Metrics
  val ex = new ScheduledThreadPoolExecutor(1)
  val gc: GremlinConnector = Service.get[GremlinConnector]
  val healthChecks: Runnable = new Runnable {
    def run(): Unit = {

      import scala.concurrent.duration._
      // will fail if the graph is empty
      val healthReportJG = try {
        Await.result(gc.g.V().limit(1).promise(), 50.millis) match {
          case Nil => HealthReport(HealthChecks.JANUSGRAPH, "fail", isUp = false, Calendar.getInstance().getTime)
          case ::(_, _) => HealthReport(HealthChecks.JANUSGRAPH, "ok", isUp = true, Calendar.getInstance().getTime)
        }
      } catch {
        case e: Throwable => HealthReport(HealthChecks.JANUSGRAPH, e.getMessage, isUp = false, Calendar.getInstance().getTime)
      }
      DefaultHealthAggregator.updateHealth(HealthChecks.JANUSGRAPH, healthReportJG)

      val healthReportConsumer = HealthReport(HealthChecks.KAFKA_CONSUMER, "ok", isUp = true, Calendar.getInstance().getTime)
      DefaultHealthAggregator.updateHealth(HealthChecks.KAFKA_CONSUMER, healthReportConsumer)
      val healthReportProducer = try {
        production.getProducerAsOpt match {
          case Some(producer) =>
            val productionMetrics = producer.metrics().asScala
            HealthUtil.processKafkaMetrics("KafkaProducerErrorTopic", productionMetrics, connectionCountMustBeNonZero = false)
          case None =>
            HealthReport(HealthChecks.KAFKA_PRODUCER, "Producer not started but OK", isUp = true, Calendar.getInstance().getTime)
        }
      } catch {
        case e: Throwable =>
          HealthReport(HealthChecks.KAFKA_PRODUCER, e.getMessage, isUp = false, Calendar.getInstance().getTime)
      }
      DefaultHealthAggregator.updateHealth(HealthChecks.KAFKA_PRODUCER, healthReportProducer)
    }
  }
  ex.scheduleAtFixedRate(healthChecks, 1, 1, TimeUnit.SECONDS)
}

/**
  * Private methods put in the object in order to be able to test them
  */
object AbstractDiscoveryService {

  /**
    * From a relation, return unique vertices
    */
  private def getDistinctVertices(relations: Seq[Relation])(implicit propSet: Set[Property]): List[VertexCore] = {
    val firstCheck = relations.flatMap(r => List(r.vFrom, r.vTo)).distinct.toList

    var mutableListOfProcessedVerticesUntilNow = new ListBuffer[VertexCore]()

    for (newV <- firstCheck) {
      mutableListOfProcessedVerticesUntilNow.find(v => v.equalsUniqueProperty(newV)) match {
        case Some(foundSameVertex) =>
          mutableListOfProcessedVerticesUntilNow -= foundSameVertex
          mutableListOfProcessedVerticesUntilNow += foundSameVertex.mergeWith(newV)
        case None =>
          mutableListOfProcessedVerticesUntilNow += newV
      }
    }
    mutableListOfProcessedVerticesUntilNow.toList

  }

  /**
    * Validate that all vertices have at least one unique property. If not, throw error
    * @param vertices vertices that will be checked
    */
  private def validateVerticesAreCorrect(vertices: List[VertexCore])(implicit propSet: Set[Property]): Option[List[VertexCore]] = {
    val res = for (v <- vertices if v.getUniqueProperties.isEmpty) yield v
    res match {
      case Nil => None
      case _ => Some(res)
    }
  }
}

@Singleton
class DefaultDiscoveryService @Inject() (storer: Storer, config: Config, lifecycle: Lifecycle, locker: Lock)(implicit val ec: ExecutionContext) extends AbstractDiscoveryService(storer, config, lifecycle, locker)
