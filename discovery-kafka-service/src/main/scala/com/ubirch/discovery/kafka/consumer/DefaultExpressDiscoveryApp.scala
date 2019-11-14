package com.ubirch.discovery.kafka.consumer

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.core.structure.Relation
import com.ubirch.discovery.core.util.Timer
import com.ubirch.discovery.kafka.metrics.{Counter, DefaultConsumerRecordsErrorCounter, DefaultConsumerRecordsSuccessCounter, MessageMetricsLoggerSummary}
import com.ubirch.discovery.kafka.models.{RelationKafka, Store}
import com.ubirch.discovery.kafka.util.ErrorsHandler
import com.ubirch.discovery.kafka.util.Exceptions.{ParsingException, StoreException}
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.express.ExpressKafkaApp
import com.ubirch.kafka.producer.ProducerRunner
import com.ubirch.niomon.healthcheck.HealthCheckServer.CheckerFn
import com.ubirch.niomon.healthcheck.{Checks, HealthCheckServer}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.jackson.Serialization

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait DefaultExpressDiscoveryApp extends ExpressKafkaApp[String, String] {

  override val producerBootstrapServers: String = conf.getString("kafkaApi.kafkaProducer.bootstrapServers")

  override val keySerializer: serialization.Serializer[String] = new StringSerializer

  override val valueSerializer: serialization.Serializer[String] = new StringSerializer

  override val consumerTopics: Set[String] = conf.getString("kafkaApi.kafkaProducer.topic").split(", ").toSet

  val producerErrorTopic: String = conf.getString("kafkaApi.kafkaConsumer.errorTopic")

  override val consumerBootstrapServers: String = conf.getString("kafkaApi.kafkaConsumer.bootstrapServers")

  override val consumerGroupId: String = conf.getString("kafkaApi.kafkaConsumer.groupId")

  override val consumerMaxPollRecords: Int = conf.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")

  override val consumerGracefulTimeout: Int = conf.getInt("kafkaApi.kafkaConsumer.gracefulTimeout")

  override val keyDeserializer: Deserializer[String] = new StringDeserializer

  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  val errorCounter: Counter = new DefaultConsumerRecordsErrorCounter
  val storeCounter: Counter = new DefaultConsumerRecordsSuccessCounter
  val messageTimeSummary = new MessageMetricsLoggerSummary

  val healthCheckServer = new HealthCheckServer(Map(), Map())
  initHealthChecks()

  case class RelationWrapper(tpe: String, data: RelationKafka)

  override def process(consumerRecords: Vector[ConsumerRecord[String, String]]): Unit = {
    consumerRecords.foreach { cr =>

      logger.debug("Received value: " + cr.value())
      storeCounter.counter.labels("ReceivedMessage").inc()
      messageTimeSummary.summary.time { () =>
        Try(parseRelations(cr.value())).recover {
          case exception: ParsingException =>
            errorCounter.counter.labels("ParsingException").inc()
            send(producerErrorTopic, ErrorsHandler.generateException(exception, cr.value()))
            logger.error(ErrorsHandler.generateException(exception, cr.value()))
            Nil
        }.filter(_.nonEmpty).map { x =>
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
      val t0 = System.nanoTime()

      // split data in batch of 8 in order to not exceed the number of gremlin pool worker * 2
      // that could create a ConnectionTimeOutException.
      if (data.size > 3) {
        val dataPartition = data.grouped(16).toList

        dataPartition foreach { batchOfAddV =>
          val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
          import scala.concurrent.ExecutionContext.Implicits.global
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
          logger.debug(s"relationship: ${x.toString}")
          Store.addV(x)
          storeCounter.counter.labels("RelationshipStoredSuccessfully").inc()
        }
      }

      val t1 = System.nanoTime()
      logger.debug(s"message MSG: ${Serialization.write(data)} of size ${data.size} processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
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
        logger.debug(s"STARTED sending a batch of ${batchOfAddV.size} asynchronously")
        val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
        import scala.concurrent.ExecutionContext.Implicits.global
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

      storeCounter.counter.labels("RelationshipStoredSuccessfully").inc(data.length)
      timer.finish(s"process CACHED message MSG: ${Serialization.write(data)} of size ${data.size} ")
      true
    } catch {
      case e: Exception =>
        logger.error("Error storing graph: " + e.getMessage)
        throw StoreException("Error storing graph: " + e.getMessage)
    }
  }

  def initHealthChecks(): Unit = {
    if (!conf.getBoolean("kafkaApi.healthcheck.enabled")) return

    def addChecksForProducerRunner(name: String, producerRunner: ProducerRunner[_, _]): Unit = {
      healthCheckServer.setReadinessCheck(name) { implicit ec =>
        producerRunner.getProducerAsOpt match {
          case Some(producer) => Checks.kafka(name, producer, connectionCountMustBeNonZero = false)._2(ec)
          case None => Checks.notInitialized(name)._2(ec)
        }
      }
    }

    def addChecksForConsumerRunner(name: String, consumerRunner: ConsumerRunner[_, _]): Unit = {
      val getConsumerWorkaround: ConsumerRunner[_, _] => Option[Consumer[_, _]] = {
        val f = consumerRunner.getClass.getField("consumer")
        f.setAccessible(true)

        { cr: ConsumerRunner[_, _] => Option(f.get(cr).asInstanceOf[Consumer[_, _]]) }
      }

      healthCheckServer.setReadinessCheck(name) { implicit ec =>
        // consumerRunner.getConsumerAsOpt match { // the library needs updating
        getConsumerWorkaround(consumerRunner) match {
          case Some(producer) => Checks.kafka(name, producer, connectionCountMustBeNonZero = false)._2(ec)
          case None => Checks.notInitialized(name)._2(ec)
        }
      }
    }

    def addReachabilityCheckForProducerRunner(checkName: String, producerRunner: ProducerRunner[_, _]): Unit = {
      def checkReachabilityForRunner(producerRunner: ProducerRunner[_, _]): CheckerFn = { ec: ExecutionContext =>
        producerRunner.getProducerAsOpt match {
          case Some(producer) => Checks.kafkaNodesReachable(producer)._2(ec)
          case None =>
            Checks.notInitialized(checkName)._2(ec)
        }
      }

      healthCheckServer.setReadinessCheck(checkName)(checkReachabilityForRunner(producerRunner))
      healthCheckServer.setLivenessCheck(checkName)(checkReachabilityForRunner(producerRunner))
    }

    healthCheckServer.setLivenessCheck(Checks.process())
    healthCheckServer.setReadinessCheck(Checks.process())

    addChecksForProducerRunner("express-kafka-producer", production)
    addChecksForConsumerRunner("express-kafka-consumer", consumption)
    addReachabilityCheckForProducerRunner("kafka-nodes-reachable", production)

    healthCheckServer.run(conf.getInt("kafkaApi.healthcheck.port"))
  }

}

