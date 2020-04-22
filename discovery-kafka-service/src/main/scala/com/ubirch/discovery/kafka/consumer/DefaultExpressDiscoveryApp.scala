package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.structure.{ Helpers, Relation, RelationServer, VertexCore, VertexDatabase }
import com.ubirch.discovery.kafka.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.kafka.models.{ KafkaElements, RelationKafka, Store }
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.express.ExpressKafkaApp
import gremlin.scala.{ Edge, Vertex }
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
import scala.collection.immutable.HashMap
import scala.concurrent.Future
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

  implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

  val maxParallelConnection: Int = conf.getInt("kafkaApi.gremlinConf.maxParallelConnection")

  override val process: Process = Process.async { crs =>
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
              case exception: Exception =>
                errorCounter.counter.labels("Unknown").inc()
                send(producerErrorTopic, ErrorsHandler.generateException(exception, cr.value()))
                logger.error(ErrorsHandler.generateException(exception, cr.value()))
                Nil

            }.getOrElse(Nil)

      }
      logger.info(s"Pooled ${crs.size} kafka messages containing ${allRelations.size} relations")
      store(allRelations).map(_ => ())

    } catch {
      case e: Exception =>
        //TODO WHAT IS OK TO LET GO
        logger.error("Error processing: ", e)
        Future.unit
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

  def store(relations: Seq[Relation]): Future[Seq[(Relation, Option[Edge])]] = {

    val hashMapVertices: HashMap[VertexCore, Option[Vertex]] = preprocess(relations)

    def getVertexFromHMap(vertexCore: VertexCore): Vertex = {
      hashMapVertices.get(vertexCore) match {
        case Some(maybeVertex) =>
          maybeVertex match {
            case Some(concreteV) => {
              concreteV
            }
            case None => Helpers.getOrCreateVertex(vertexCore).getOrElse(throw new Exception("cant add or get vertex"))
          }
        case None =>
          logger.debug(s"didn't find vertex ${vertexCore.toString} in the hashMap")
          Helpers.getOrCreateVertex(vertexCore).getOrElse(throw new Exception("cant add or get vertex"))
      }
    }

    logger.debug(s"after preprocess: hashmap size =  ${hashMapVertices.size}, relation size: ${relations.distinct.size}")

    val createdRelations: Future[Seq[(Relation, Option[Edge])]] = Future.sequence {
      relations.map { r =>
        logger.debug("adding relation")
        Helpers.createRelation(getVertexFromHMap(r.vFrom), getVertexFromHMap(r.vTo), r.edge).map(x => (r, x)).recoverWith {
          case e: StoreException =>
            errorCounter.counter.labels("StoreException").inc()
            logger.error(ErrorsHandler.generateException(e, r.toString))
            send(producerErrorTopic, ErrorsHandler.generateException(e, r.toString))
            Future.successful(r, None)
          case e: Throwable =>
            errorCounter.counter.labels("Exception").inc()
            logger.error(ErrorsHandler.generateException(e, r.toString))
            send(producerErrorTopic, ErrorsHandler.generateException(e, r.toString))
            Future.successful(r, None)
        }
      }
    }.recover {
      case e: Exception =>
        logger.error("OMG := ", e)
        throw e
    }

    createdRelations
  }

  def preprocess(relations: Seq[Relation]): HashMap[VertexCore, Option[Vertex]] = {
    // 1: flatten relations to get the vertices
    val vertices: Seq[VertexCore] = Store.getAllVerticeFromRelations(relations)

    // 2: create the hashMap [vertexCore, vertexDb]
    vertices.toList
      .foldLeft(HashMap.empty[VertexCore, Option[Vertex]])((existing, b) => existing ++ HashMap(b -> Helpers.getOrCreateVertex(b)))

  }

  def recoverStoreRelationIfNeeded(relationAndResult: (RelationServer, Future[Option[Edge]])): Future[Object] = {
    relationAndResult._2

  }

  def increasePrometheusRelationCount(): Unit = {
    storeCounter.counter.labels("RelationStoredSuccessfully").inc()
  }

}

