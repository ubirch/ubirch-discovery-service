package com.ubirch.discovery

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.models.lock.Lock
import com.ubirch.discovery.models.{ EdgeCore, Relation, VertexCore }
import com.ubirch.discovery.services.connector.GremlinConnector
import com.ubirch.discovery.util.Util
import monix.eval.Task
import monix.execution.Scheduler
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers }
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.Random

trait TestBase
  extends FeatureSpec
  with ScalaFutures
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with Matchers
  with LazyLogging
  with EmbeddedKafka {

  def await[T](future: Future[T]): T = await(future, Duration.Inf)

  def await[T](future: Future[T], atMost: Duration): T = Await.result(future, atMost)

  val random = new Random

  def giveMeRandomString: String = Random.alphanumeric.take(64).mkString

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

  def generateElementProperty(key: String, value: String = giveMeRandomString) = {
    Util.convertProp(key, value)
  }

  def giveMeATimestamp: String = System.currentTimeMillis.toString

  def giveMeRandomVertexLabel: String = listLabelsVertex(random.nextInt(listLabelsVertex.length))
  def giveMeRandomEdgeLabel: String = listLabelsEdge(random.nextInt(listLabelsEdge.length))

  val listLabelsEdge = List("UPP->DEVICE", "CHAIN", "MASTER_TREE->SLAVE_TREE", "SLAVE_TREE->SLAVE_TREE", "PUBLIC_CHAIN->MASTER_TREE")

  val listLabelsVertex = List("DEVICE", "UPP", "MASTER_TREE", "SLAVE_TREE", "PUBLIC_CHAIN")

  def waitUntilRedisStart(lock: Lock, atMost: Duration = 5.seconds)(implicit scheduler: Scheduler): Unit = {
    def wait(left: Duration): Task[Unit] = {
      val connected = lock.isConnected
      for {
        _ <- if (connected) {
          Task.unit
        } else if (!connected && left.toMillis > 0) {
          Task.sleep(100.millis).flatMap(_ => wait(left - 100.millis))
        } else {
          Task.raiseError(new RuntimeException(
            s"Could not complete specified task due to timeout"
          ))
        }
      } yield ()
    }
    wait(atMost).runSyncUnsafe(atMost)
  }
}
