package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.{ DumbRelation, ElementProperty, Relation, VertexCore }
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.{ Helpers, Timer }
import com.ubirch.discovery.kafka.metrics.{ Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter }
import com.ubirch.discovery.kafka.models.{ Executor, KafkaElements, RelationKafka }
import com.ubirch.discovery.kafka.util.{ ErrorsHandler, RedisCache }
import com.ubirch.discovery.kafka.util.Exceptions.{ ParsingException, StoreException }
import com.ubirch.kafka.express.ExpressKafkaApp
import gremlin.scala.Vertex
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer, StringSerializer }
import org.json4s._

import scala.collection.immutable
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

  val maxParallelConnection: Int = conf.getInt("kafkaApi.gremlinConf.maxParallelConnection") // PUT AT 1 FOR TESTS

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

    // FOR TESTS: val preprocessBatchSize = 1 + scala.util.Random.nextInt(100)
    val res = Timer.time({

      val hashMapVertices: Map[VertexCore, Vertex] = preprocess(relations, 10)

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
        //val verticesNumber = Store.getAllVerticeFromRelations(relations).toList.size
        logger.info(s"processed {${relations.size},${res.elapsed.toDouble / relations.size.toDouble},${res.elapsed}}")
        success
      case Failure(exception) =>
        logger.error("Error storing relations, out of executor", exception)
        throw StoreException("Error storing relations, out of executor", exception)
    }
  }

  def preprocess(relations: Seq[Relation], batchSize: Int): Map[VertexCore, Vertex] = {
    // 1: flatten relations to get the vertices
    val vertices: List[VertexCore] = getAllVerticeFromRelations(relations).toList

    try {
      if (RedisCache.isRedisUp) {
        redisPreprocess(vertices, batchSize)
      } else {
        noRedisPreprocess(vertices, batchSize)
      }
    } catch {
      case _: Throwable =>
        logger.warn("can not connect to redis")
        noRedisPreprocess(vertices, batchSize)
    }
  }

  def getAllVerticeFromRelations(relations: Seq[Relation]): Seq[VertexCore] = {
    relations.flatMap(r => List(r.vFrom, r.vTo)).distinct
  }

  def redisPreprocess(vertices: List[VertexCore], batchSize: Int): Map[VertexCore, Vertex] = {

    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate

    val verticesWithHash: List[VertexCore] = vertices.filter(v => v.properties.exists(p => p.keyName.eq("hash")))
    val verticesWithoutHash: List[VertexCore] = vertices.filter(v => !v.properties.exists(p => p.keyName.eq("hash")))
    logger.debug("vertices without hash: " + verticesWithoutHash.mkString("; "))
    val resFromRedis = verticesWithHash.map { v =>
      {
        val hash = maybeVertexHash(v).get.value.toString
        v -> getVertexFromHash(hash)
      }
    }
    //success = exist on redis, has an id on redis, and are complete on redis
    val res: immutable.Seq[(VertexCore, Option[String])] = resFromRedis.map { vm =>
      vm._2 match {
        case Some(map) =>
          getRedisVertexId(map) match {
            case Some(value) =>
              if (doesRedisAnswerHasAtLeastAllValues(map, vm._1)) {
                (vm._1, Some(value))
              } else {
                logger.debug("/!\\ VERTEX NOT COMPLETE REDIS: " + vm._1.toString)
                (vm._1, None)
              }
            case None =>
              logger.debug("/!\\ VERTEX NO ID REDIS: " + vm._1.toString)
              (vm._1, None)
          }
        case None =>
          logger.debug("/!\\ VERTEX NOT FOUND REDIS: " + vm._1.toString)
          (vm._1, None)
      }
    }

    val redisSuccess: immutable.Seq[(VertexCore, String)] = res.filter(p => p._2.isDefined).map(vi => (vi._1, vi._2.get))
    val redisFailures = res.filter(p => p._2.isEmpty).map(vi => vi._1)

    val redisSuccessAsVertices: Map[VertexCore, Vertex] = {
      if (redisSuccess.nonEmpty) {
        logger.debug("entering executor redisSuccessAsVertices")
        val executor = new Executor[(VertexCore, String), Vertex](objects = redisSuccess, f = Helpers.idToVertex, processSize = maxParallelConnection)
        executor.startProcessing()
        logger.debug("Waiting for executor redisSuccessAsVertices")
        executor.latch.await()
        executor.getResultsNoTry.map { r => r._1._1 -> r._2 }.toMap
      } else Map.empty
    }

    val verticesNotCompleteOnRedisToPreprocess: immutable.List[VertexCore] = verticesWithoutHash ++ redisFailures

    logger.info("Vertices not on redis / on redis: " + verticesNotCompleteOnRedisToPreprocess.size + "/" + redisSuccess.size)
    val verticeToPreprocess: Seq[List[VertexCore]] = verticesNotCompleteOnRedisToPreprocess.grouped(batchSize).toSeq

    if (verticeToPreprocess.nonEmpty) {
      val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](objects = verticeToPreprocess, f = Helpers.getUpdateOrCreateMultiple(_), processSize = maxParallelConnection)
      executor.startProcessing()
      executor.latch.await()
      val j = executor.getResultsNoTry
      val theRes: Map[VertexCore, Vertex] = j.flatMap(r => r._2).toMap
      theRes foreach { v =>
        maybeVertexHash(v._1) match {
          case Some(hashProp) => {
            logger.debug("updating vertice on redis: " + v._1.toString)
            updateVertexOnRedis(hashProp.value.toString, getAllPropsExceptHash(v._1) ++ Map("vertexId" -> v._2.id().toString))
          }
          case None =>
        }
      }
      theRes ++ redisSuccessAsVertices
    } else {
      redisSuccessAsVertices
    }
  }

  def noRedisPreprocess(vertices: List[VertexCore], batchSize: Int): Map[VertexCore, Vertex] = {
    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate


    val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](objects = vertices.grouped(batchSize).toSeq, f = Helpers.getUpdateOrCreateMultiple(_), processSize = maxParallelConnection)
    executor.startProcessing()
    executor.latch.await()
    val j = executor.getResultsNoTry
    j.flatMap(r => r._2).toMap
  }

  def getAllPropsExceptHash(vertexCore: VertexCore): Map[String, String] = {
    vertexCore.properties.filter(p => p.keyName != "hash").map { p => p.keyName -> p.value.toString }.toMap
  }

  def maybeVertexHash(vertexCore: VertexCore): Option[ElementProperty] = vertexCore.properties.find(p => p.keyName.equals("hash"))

  def getVertexFromHash(hash: String): Option[Map[String, String]] = {
    RedisCache.getAllFromHash(hash)
  }

  def getRedisVertexId(redisValue: Map[String, String]): Option[String] = {
    redisValue.get("vertexId")
  }

  def doesRedisAnswerHasAtLeastAllValues(redisAnswer: Map[String, String], vertexCore: VertexCore): Boolean = {

    var notSameValues: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

    for (vertexProperty <- vertexCore.properties.filter(p => p.keyName != "hash")) {
      redisAnswer.get(vertexProperty.keyName) match {
        case Some(redisValue) => if (!redisValue.equals(vertexProperty.value.toString)) {
          logger.debug("notSameValues: should be" + vertexProperty.keyName + "->" + vertexProperty.value.toString + " but on redis is: " + redisValue)
          notSameValues = notSameValues += (vertexProperty.keyName -> vertexProperty.value.toString)
        }
        case None =>
          logger.debug("notSameValues: doesn't exist on redis: " + vertexProperty.keyName + "->" + vertexProperty.value.toString)
          notSameValues = notSameValues += (vertexProperty.keyName -> vertexProperty.value.toString)
      }
    }
    notSameValues.isEmpty
  }

  def updateVertexOnRedis(hash: String, thingsToUpdate: Map[String, String]): Boolean = {
    logger.debug("things to update: " + thingsToUpdate.mkString("; "))
    RedisCache.updateVertex(hash, thingsToUpdate)
  }

  def createVertexOnRedis(hash: String, properties: List[ElementProperty]): Boolean = {
    RedisCache.updateVertex(hash, properties.filter(p => p.keyName != "hash").map { p => p.keyName -> p.value.toString }.toMap)
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

