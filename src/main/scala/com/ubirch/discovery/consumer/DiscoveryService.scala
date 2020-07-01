package com.ubirch.discovery.consumer

import com.typesafe.config.Config
import com.ubirch.discovery.{ DiscoveryError, Lifecycle }
import com.ubirch.discovery.models._
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.process.Executor
import com.ubirch.discovery.services.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.discovery.util.Timer
import com.ubirch.discovery.ConfPaths.{ ConsumerConfPaths, DiscoveryConfPath, ProducerConfPaths }
import com.ubirch.kafka.express.ExpressKafka
import gremlin.scala.Vertex
import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
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

abstract class AbstractDiscoveryService(storer: Storer, config: Config, lifecycle: Lifecycle) extends DiscoveryApp
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

  //  val healthCheckServer = new HealthCheckServer(Map(), Map())
  //  initHealthChecks()

  override val process: Process = Process { crs => letsProcess(crs) }

  /*
  * Logic of process put as a separate method in order to test without invoking kafka
    */
  def letsProcess(crs: Vector[ConsumerRecord[String, String]]): Unit = {
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
      Some(relationAsInternalStruct map { r => r.toCoreRelation })
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

    val timedResult = Timer.time({

      preprocess(relations) match {
        case Some(hashMap) =>
          val hashMapVertices: Map[VertexCore, Vertex] = hashMap

          def getVertexFromHMap(vertexCore: VertexCore): Vertex = {
            hashMapVertices.get(vertexCore) match {
              case Some(vDb) => vDb
              case None =>
                logger.info(s"getVertexFromHMap vertex not found in HMAP ${vertexCore.toString}")
                storer.getUpdateOrCreateSingleConcrete(vertexCore)
            }
          }

          logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.size}")
          val relationsAsRelationServer: Seq[DumbRelation] = relations.map(r => DumbRelation(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge))

          val executor = new Executor[DumbRelation, Any](objects = relationsAsRelationServer, f = storer.createRelation, processSize = maxParallelConnection, customResultFunction = Some(() => this.increasePrometheusRelationCount()))
          executor.startProcessing()
          executor.latch.await(100, java.util.concurrent.TimeUnit.SECONDS)
          Some(executor.getResults)
        case None => None
      }

    })
    //res.logTimeTakenJson(s"process_relations" -> List(("size" -> relations.size) ~ ("value" -> relations.map { r => r.toJson }.toList)), 10000, warnOnly = false)

    timedResult.result match {
      case Success(success) =>
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
  def preprocess(relations: Seq[Relation]): Option[Map[VertexCore, Vertex]] = {
    // 1: flatten relations to get the vertices
    val distinctVertices: List[VertexCore] = getDistinctVertices(relations)
    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
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
        Some(j.flatMap(r => r._2).toMap)
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
    logger.error(errorMessage, ex.getMessage, ex)
    val producerRecordToSend = new ProducerRecord[String, String](
      producerErrorTopic,
      DiscoveryError(errorMessage, ex.getClass.getSimpleName, value).toString
    )
    send(producerRecordToSend)
      .recover { case _ => logger.error(s"failure publishing to error topic: $errorMessage") }
  }

  protected def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = production.send(producerRecord)

  lifecycle.addStopHook { () =>
    logger.info("Shutting down kafka")
    Future.successful(consumption.shutdown(consumerGracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
  }

}

object AbstractDiscoveryService {

  /**
    * From a relation, return unique vertices
    */
  private def getDistinctVertices(relations: Seq[Relation]): List[VertexCore] =
    relations.flatMap(r => List(r.vFrom, r.vTo)).distinct.toList

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
class DefaultDiscoveryService @Inject() (storer: Storer, config: Config, lifecycle: Lifecycle)(implicit val ec: ExecutionContext) extends AbstractDiscoveryService(storer, config, lifecycle)
