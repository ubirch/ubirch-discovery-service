package com.ubirch.discovery.kafka

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.structure.{ EdgeCore, ElementProperty, Relation, VertexCore }
import com.ubirch.discovery.core.ExecutionContextHelper
import com.ubirch.discovery.kafka.util.Util
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers }

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Random

trait TestBase
  extends FeatureSpec
  with ScalaFutures
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with Matchers
  with LazyLogging
  with EmbeddedKafka {

  implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  def await[T](future: Future[T]): T = await(future, Duration.Inf)

  def await[T](future: Future[T], atMost: Duration): T = Await.result(future, atMost)

  val random = new Random

  def giveMeRandomString: String = Random.alphanumeric.take(32).mkString

  def generateRelation = Relation(generateVertex, generateVertex, generateEdge)

  def generateEdge: EdgeCore = {
    val label = giveMeRandomEdgeLabel
    EdgeCore(Nil, label)
      .addProperty(generateElementProperty("timestamp", giveMeATimestamp))
  }

  def generateVertex: VertexCore = {
    val label = giveMeRandomVertexLabel
    VertexCore(Nil, label)
      .addProperty(generateElementProperty("hash"))
      .addProperty(generateElementProperty("signature"))
      .addProperty(generateElementProperty("timestamp", giveMeATimestamp))
  }

  def generateElementProperty(key: String, value: String = giveMeRandomString): ElementProperty = {
    Util.convertProp(key, value)
  }

  def giveMeATimestamp: String = System.currentTimeMillis.toString

  def giveMeRandomVertexLabel: String = listLabelsVertex(random.nextInt(listLabelsVertex.length))
  def giveMeRandomEdgeLabel: String = listLabelsEdge(random.nextInt(listLabelsEdge.length))

  val listLabelsEdge = List("transaction", "link", "associate", "father", "generate")

  val listLabelsVertex = List("DEVICE", "UPP", "MASTER_TREE", "SLAVE_TREE", "PUBLIC_CHAIN")

}
