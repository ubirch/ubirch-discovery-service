package com.ubirch.discovery.kafka.consumer

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.kafka.TestBase
import com.ubirch.kafka.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.ExecutionContext
import scala.io.Source

//TODO: We need to rethink the tests here are they are causing issues on the ci pipelines
class DefaultStringConsumerSpec extends TestBase {

  val topic = "test"
  val errorTopic = "com.ubirch.eventlog.discovery-error"
  implicit val Deserializer: StringDeserializer = new StringDeserializer

  feature("Verifying valid requests") {

    def runTest(test: TestStruct): Unit = {

      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      withRunningKafka {

        val consumer = new DefaultExpressDiscoveryApp {
          override implicit def ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
          override def prefix: String = "Ubirch"
          override def maxTimeAggregationSeconds: Long = 180
        }
        consumer.consumption.setForceExit(false)
        consumer.consumption.start()
        cleanDb()
        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(4000)
        howManyElementsInJG shouldBe howManyElementsShouldBeInJg(test.expectedResult)
        consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)
      }
    }

    val allTests = getAllTests("/valid/")

    ignore("NeedForJanus") {

      allTests foreach { test =>
        scenario(test.nameOfTest) {
          runTest(test)
        }
      }
    }

  }

  feature("Invalid requests: Parsing errors") {

    def runTest(test: TestStruct): Unit = {
      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      withRunningKafka {

        val consumer = new DefaultExpressDiscoveryApp {
          override implicit def ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
          override def prefix: String = "Ubirch"
          override def maxTimeAggregationSeconds: Long = 180
        }
        consumer.consumption.setForceExit(false)
        consumer.consumption.start()
        cleanDb()
        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(100)
        consumeFirstMessageFrom(errorTopic) shouldBe test.expectedResult
        consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)
      }
    }

    val allTests = getAllTests("/invalid/parsing/")

    ignore("NeedForJanus") {

      allTests foreach { test =>
        scenario(test.nameOfTest) {
          runTest(test)
        }
      }

    }

  }

  feature("Invalid requests: Storing errors") {

    def runTest(test: TestStruct): Unit = {

      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      withRunningKafka {

        val consumer = new DefaultExpressDiscoveryApp {
          override implicit def ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
          override def prefix: String = "Ubirch"
          override def maxTimeAggregationSeconds: Long = 180
        }
        consumer.consumption.setForceExit(false)
        consumer.consumption.start()
        cleanDb()
        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(4000)
        consumeFirstMessageFrom(errorTopic) shouldBe test.expectedResult
        howManyElementsInJG shouldBe (0, 0)
        consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)
      }
    }

    val allTests = getAllTests("/invalid/storing/")

    ignore("NeedForJanus") {

    allTests foreach { test =>
      scenario(test.nameOfTest) {
        runTest(test)
      }
    }

    }

  }

  //   ------ helpers -------

  def getDefaultEmbeddedKafkaConfig: EmbeddedKafkaConfig = {
    EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = PortGiver.giveMeZookeeperPort)
  }

  case class TestStruct(request: String, expectedResult: String, nameOfTest: String)

  /*
  Get a List of TestStruct containing :
  - the request to send
  - the expected result
  - the name of the test
  Does so by reading the tests in directory/inquiry and directory/expectedResults, zip the results, and gives the test
  the same name as its file name
   */
  def getAllTests(directory: String): List[TestStruct] = {
    val filesReq: List[File] = getFilesInDirectory(directory + "inquiry/")
    val filesExpectedResults: List[File] = getFilesInDirectory(directory + "expectedResults/")

    val allExpectedResults: List[(String, String)] = filesExpectedResults map { f =>
      (readFile(f.getCanonicalPath).head, f.getName)
    }

    val allRes: List[String] = filesReq map { f =>
      readFile(f.getCanonicalPath).head
    }
    val res: List[(String, (String, String))] = allRes zip allExpectedResults

    res map { m => TestStruct(m._1, m._2._1, m._2._2) }
  }

  /**
    * Return all the first lines of all the files in the specified directory as a List of String.
    * @param directory The directory where the files will be read.
    * @return A list of String representing all the first lines of all the files in the specified directory.
    */
  def readAllFiles(directory: String): List[String] = {
    val listFiles = getFilesInDirectory(directory)
    listFiles map { f => readFile(f.getCanonicalPath).head }
  }

  def getFilesInDirectory(dir: String): List[File] = {
    val path = getClass.getResource(dir)
    val folder = new File(path.getPath)
    val res: List[File] = if (folder.exists && folder.isDirectory) {
      folder.listFiles
        .toList
    } else Nil
    res
  }

  def readFile(nameOfFile: String): List[String] = {
    val source = Source.fromFile(nameOfFile)
    val lines = source.getLines.toList
    source.close
    lines
  }

  def getGremlinConnector: GremlinConnector = {
    GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)
  }

  def cleanDb(): Unit = {
    val gc = getGremlinConnector
    gc.g.V().drop().iterate()
  }

  /**
    * Determine how many elements (vertex and edges) are stored in janusgraph.
    * @return tuple(numberOfVertex: Int, numberOfEdges: Int).
    */
  def howManyElementsInJG(): (Int, Int) = {
    val gc = getGremlinConnector
    val numberOfVertices = gc.g.V().count().toList().head.toInt
    val numberOfEdges = gc.g.E().count().toList().head.toInt
    (numberOfVertices, numberOfEdges)
  }

  def howManyElementsShouldBeInJg(values: String): (Int, Int) = {
    val nVertices = values.substring(0, values.indexOf(",")).toInt
    val nEdges = values.substring(values.indexOf(",") + 1).toInt
    (nVertices, nEdges)
  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
  }

}
