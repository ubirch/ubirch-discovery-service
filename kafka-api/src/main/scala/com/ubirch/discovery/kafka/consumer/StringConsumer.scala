package com.ubirch.discovery.kafka.consumer

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.{AddVertices, GremlinConnector}
import com.ubirch.kafka.consumer.{Configs, ConsumerRecordsController, ProcessResult, StringConsumer, WithMetrics}
import gremlin.scala.{Key, KeyValue}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

object StringConsumer extends LazyLogging{


  val topics: Set[String] = Set("test")

  val configs = Configs(
    bootstrapServers = "localhost:9092",
    groupId = "my group id",
    enableAutoCommit = false,
    autoOffsetReset = OffsetResetStrategy.EARLIEST,
    maxPollRecords = 500
  )

  val myController: ConsumerRecordsController[String, String] {
    type A = ProcessResult[String, String]
  } = new ConsumerRecordsController[String, String] {

    override type A = ProcessResult[String, String]

    override def process(consumerRecord: Vector[ConsumerRecord[String, String]]): Future[ProcessResult[String, String]] = {
      consumerRecord.foreach { cr =>
        logger.info(cr.value())
        path(cr.value())
      }

      Future.successful(new ProcessResult[String, String] {
        override val id: UUID = UUID.randomUUID()
        override val consumerRecords: Vector[ConsumerRecord[String, String]] = consumerRecord
      })
    }
  }

  lazy val consumerConfigured: StringConsumer with WithMetrics[String, String] = {
    val consumerImp = new StringConsumer() with WithMetrics[String, String]
    consumerImp.setUseAutoCommit(false)
    consumerImp.setTopics(topics)
    consumerImp.setProps(configs)
    consumerImp.setKeyDeserializer(Some(new StringDeserializer()))
    consumerImp.setValueDeserializer(Some(new StringDeserializer()))
    consumerImp.setConsumerRecordsController(Some(myController))
    consumerImp
  }


  def path(data: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val result = try {
      parse(data)
    } catch {
      case e: Throwable => logger.error("error", e.getMessage)
    }
    result match {
      case x: JValue => findPath(x)
      case _ =>
    }

  }

  case class AddV(v1: Vertounet, v2: Vertounet, edge: Edgounet)

  case class Vertounet(id: String, properties: Map[String, String], label: String = "aLabel")

  case class Edgounet(properties: Map[String, String])

  def mapToListKeyValues(propMaps: Map[String, String]): List[KeyValue[String]] = propMaps map { x => KeyValue(Key(x._1), x._2) } toList

  implicit val gc: GremlinConnector = new GremlinConnector

  /**
    * Entry should be formatted as the following:
    * {"v1":{
    * "id": "ID"
    * "label": "label" OPTIONAL
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * }
    * "v2":{
    * "id": "ID"
    * "label": "label" OPTIONAL
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * }
    * "edge":{
    * "properties":{
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * }}}
    *
    * @param req The parsed JSON
    * @return
    */
  def findPath(req: JValue): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val didItWork = try {
      listAddV(req.extract[List[AddV]])
      true
    } catch {
      case e: Throwable =>
        logger.info("not a list of stuff")
        false
    }
    if (!didItWork) {
      req.extract[AddV] match {
        case null => "error"
        case x =>
          addV(x)
          "okidoki"
      }
    }

  }

  def addV(req: AddV): Unit = {
    val id1 = req.v1.id
    val p1 = mapToListKeyValues(req.v1.properties)
    val l1 = req.v1.label
    val id2 = req.v2.id
    val p2 = mapToListKeyValues(req.v2.properties)
    val l2 = req.v2.label
    val pE = mapToListKeyValues(req.edge.properties)
    new AddVertices().addTwoVertices(id1, p1, l1)(id2, p2, l2)(pE)
  }

  def listAddV(l: List[AddV]): Unit = {
    l foreach (x => addV(x))
  }



  def start(): Unit = {
    consumerConfigured.start()
  }

}
