package com.ubirch.discovery.kafka.models

import java.util.concurrent.{CountDownLatch, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.structure.Relation
import com.ubirch.discovery.kafka.TestBase

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ExecutorSpec extends TestBase {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10, CustomThreadFactory))

  /**
    * Simple dummy operations to "warm-up" the connection between the spec and JanusGraph
    */
  def warmUpJg = {
    gc.g.V().limit(1)
    executeAllJanus(1)
    executeAllJanus(1)
    executeAllJanus(1)
  }
  def cleanUpJanus = {
    while (gc.g.V().count().l().head != 0) {
      gc.g.V().limit(1000).drop().iterate()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    //cleanUpJanus
    warmUpJg
    Thread.sleep(4000)
  }

  feature("benchmarks on adding a relation") {

    scenario("1 relations") {
      executeAllJanus(1)
    }

    scenario("4 relations") {
      executeAllJanus(4)
    }

    scenario("5 relations") {
      executeAllJanus(5)
    }

    scenario("6 relations") {
      executeAllJanus(6)
    }

    scenario("7 relations") {
      executeAllJanus(7)
    }

    scenario("10 relations") {
      executeAllJanus(10)
    }

    scenario("50 relations") {
      executeAllJanus(50)
    }

    scenario("100 relations") {
      executeAllJanus(100)
    }

    scenario("200 relations") {
      executeAllJanus(200)
    }

    scenario("500 relations") {
      executeAllJanus(500)
    }

  }

  feature("benchmark on other things") {
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

  def executeAllJanus(number: Int): Unit = {
    // generate objects
    val objects1: immutable.Seq[Relation] = for (_ <- 0 until number) yield generateRelation
    val objects2: immutable.Seq[Relation] = for (_ <- 0 until number) yield generateRelation

    // new style
    val t0_0 = System.currentTimeMillis()
    execute[Relation, Unit](objects1, Store.addRelation)
    val t0_1 = System.currentTimeMillis()
    logger.info(s"NEW STYLE - Took ${t0_1 - t0_0} ms to process $number relations => ${(t0_1 - t0_0) / number} ms/relation")
    Thread.sleep(1000)

    // old style
    val t1_0 = System.currentTimeMillis()
    executeOldStyle(objects2, Store.addRelation)
    val t1_1 = System.currentTimeMillis()
    logger.info(s"OLD STYLE - Took ${t1_1 - t1_0} ms to process $number relations => ${(t1_1 - t1_0) / number} ms/relation")
    Thread.sleep(1000)
  }

  def executeAllTime(number: Int): Unit = {
    import scala.util.Random

    val objects = Random.shuffle((0 to number).toList)

    def f(timeToWait: Int): Unit = Thread.sleep(timeToWait)

    val t0_0 = System.currentTimeMillis()
    execute[Int, Unit](objects, f)
    val t0_1 = System.currentTimeMillis()
    println(s"NEW STYLE - Took ${t0_1 - t0_0} ms to process $number f() => ${(t0_1 - t0_0) / number} ms/process")
    Thread.sleep(1000)

    val t1_0 = System.currentTimeMillis()
    executeOldStyle(objects, f)
    val t1_1 = System.currentTimeMillis()
    println(s"OLD STYLE - Took ${t1_1 - t1_0} ms to process $number f() => ${(t1_1 - t1_0) / number} ms/process")
    Thread.sleep(1000)

  }

  def execute[T, U](objects: Seq[T], f: T => U): Unit = {
    val executor = new Executor[T, U](objects, f, 8)
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

