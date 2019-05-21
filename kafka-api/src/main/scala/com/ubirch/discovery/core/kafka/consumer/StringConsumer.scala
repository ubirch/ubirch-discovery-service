package com.ubirch.discovery.core.kafka.consumer

import java.util.UUID

import com.ubirch.discovery.core.{AddVertices, GremlinConnector}
import com.ubirch.kafka.consumer.{Configs, ConsumerRecordsController, ProcessResult, StringConsumer, WithMetrics}
import gremlin.scala.{Key, KeyValue}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

object StringConsumer extends App {

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  val topics = Set("test")

  val configs = Configs(
    bootstrapServers = "localhost:9092",
    groupId = "my group id",
    enableAutoCommit = false,
    autoOffsetReset = OffsetResetStrategy.EARLIEST,
    maxPollRecords = 500
  )

  val myController = new ConsumerRecordsController[String, String] {

    override type A = ProcessResult[String, String]

    override def process(consumerRecord: Vector[ConsumerRecord[String, String]]): Future[ProcessResult[String, String]] = {
      consumerRecord.foreach { cr =>
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

  consumerConfigured.startPolling()
  while (true) {

  }

  def path(data: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val requestType = data.substring(0, 9)

    val result = try {
      parse(data.substring(9))
    } catch {
      case _: Throwable => log.error("error")
    }
    result match {
      case x: JValue =>
        requestType match {
          case "addVertex" => addVertices(x)
        }
      case _ =>
    }

  }

  case class AddV(v1: Vertounet, v2: Vertounet, edge: Edgounet)

  case class Vertounet(id: String, properties: Map[String, String])

  case class Edgounet(properties: Map[String, String])

  def mapToListKeyValues(propMaps: Map[String, String]): List[KeyValue[String]] = propMaps map { x => KeyValue(Key(x._1), x._2) } toList

  implicit val gc: GremlinConnector = new GremlinConnector

  /**
    * Entry should be formatted as the following:
    * {"v1":{
    * "id": "ID"
    * "properties": {
    * "prop1Name": "prop1Value",
    * ...
    * "propNName": "propNValue"
    * }
    * "v2":{
    * "id": "ID"
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
  def addVertices(req: JValue) = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val addVertexounet = req.extract[AddV]
    val id1 = addVertexounet.v1.id
    val p1 = mapToListKeyValues(addVertexounet.v1.properties)
    val id2 = addVertexounet.v2.id
    val p2 = mapToListKeyValues(addVertexounet.v1.properties)
    val pE = mapToListKeyValues(addVertexounet.edge.properties)
    new AddVertices().addTwoVertices(id1, p1)(id2, p2)(pE)
  }

  def getVertices(req: JValue): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

  }

}
