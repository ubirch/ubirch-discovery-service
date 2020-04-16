package com.ubirch.discovery.kafka.models

import java.util.concurrent.{ CountDownLatch, Executors, ThreadFactory }
import java.util.concurrent.atomic.AtomicInteger

import com.ubirch.discovery.core.ExecutionContextHelper
import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.Relation
import com.ubirch.discovery.kafka.TestBase

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

class ExecutorSpec extends TestBase {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  /**
    * Simple dummy operations to "warm-up" the connection between the spec and JanusGraph
    */
  def warmUpJg(): Unit = {
    gc.g.V().limit(1)
  }
  def cleanUpJanus(): Unit = {
    while (gc.g.V().count().l().head != 0) {
      gc.g.V().limit(1000).drop().iterate()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    //cleanUpJanus
    warmUpJg()
    Thread.sleep(4000)
  }

  ignore("benchmark the executor on random time") {
    scenario("1") {
      executeAllTime(1)
    }

    scenario("5") {
      executeAllTime(5)
    }

    scenario("10") {
      executeAllTime(10)
    }

    scenario("20") {
      executeAllTime(20)
    }

    scenario("50") {
      executeAllTime(50)
    }

    scenario("100") {
      executeAllTime(100)
    }

    scenario("200") {
      executeAllTime(200)
    }

    scenario("500") {
      executeAllTime(500)
    }

  }

  def executeAllTime(number: Int): Unit = {
    import scala.util.Random

    val objects = Random.shuffle((0 to number).toList)

    def f(timeToWait: Int): Unit = Thread.sleep(timeToWait)

    val t0_0 = System.currentTimeMillis()
    execute[Int, Unit](objects, f)
    val t0_1 = System.currentTimeMillis()
    //println(s"NEW STYLE - Took ${t0_1 - t0_0} ms to process $number f() => ${(t0_1 - t0_0) / number} ms/process")
    Thread.sleep(1000)

    val t1_0 = System.currentTimeMillis()
    executeOldStyle(objects, f)
    val t1_1 = System.currentTimeMillis()
    //println(s"OLD STYLE - Took ${t1_1 - t1_0} ms to process $number f() => ${(t1_1 - t1_0) / number} ms/process")
    Thread.sleep(1000)

  }

  def execute[T, U](objects: Seq[T], f: T => U): Unit = {
    val executor = new Executor[T, U](objects.map { o => (o, f) }, 8)
    executor.startProcessing()
    executor.latch.await()
  }

  def executeOldStyle[T, U](objects: Seq[T], f: T => U): Unit = {

    val relationsPartition: immutable.Seq[Seq[T]] = objects.grouped(8).toList
    relationsPartition foreach { batchOfRelations =>
      val processesOfFutures = scala.collection.mutable.ListBuffer.empty[Future[U]]
      batchOfRelations.foreach { relation =>
        val process = Future(f(relation))
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

  }

}

object CustomThreadFactory extends ThreadFactory {

  private val poolNumber = new AtomicInteger(1)
  private val threadNumber = new AtomicInteger(1)

  val s: SecurityManager = System.getSecurityManager
  val group: ThreadGroup = if (s != null) { s.getThreadGroup } else { Thread.currentThread.getThreadGroup }

  val namePrefix: String = "executor-" + poolNumber.getAndIncrement + "-thread-"

  override def newThread(r: Runnable): Thread = {
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
    if (t.isDaemon) t.setDaemon(false)
    if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
    t
  }
}

