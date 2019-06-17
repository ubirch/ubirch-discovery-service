package com.ubirch.discovery.kafka.consumer

import java.io.File

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.kafka.TestBase
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.breakOut
import scala.io.Source

class DefaultStringConsumerSpec extends TestBase {

  val topic = "test"
  val errorTopic = "com.ubirch.eventlog.discovery-error"
  implicit val Deserializer: StringDeserializer = new StringDeserializer


  feature("Verifying valid requests") {

    def runTest(test: ValidTest): Unit = {

      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      withRunningKafka {

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()
        cleanDb()
        logger.info("coucou")
        logger.info(test.request)
        logger.info(getGremlinConnector.g.V.count().toList().head.toString)
        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(4000)
        howManyElementsInJG shouldBe howManyElementsShouldBeInJg(test.expectedResult)

      }

    }

    val allTests = getAllValidTests("/valid/")

    allTests foreach { test =>
        scenario(test.nameOfTest) { runTest(test) }
    }

  }

  feature("Verifying invalid requests") {
    scenario("Parsing errors") {
      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      cleanDb()
      withRunningKafka {
        val allRequests: Seq[String] = readAllFiles("/invalid/requests/parsing/")
        val allExpectedResults: Seq[String] = readAllFiles("/invalid/expectedResults/parsing/")
        val mapReqExpected: Map[String, String] = (allRequests zip allExpectedResults)(breakOut): Map[String, String]

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()

        mapReqExpected.foreach { re =>
          publishStringMessageToKafka(topic, re._1)
          Thread.sleep(100)
          consumeFirstMessageFrom(errorTopic) shouldBe re._2
        }

        howManyElementsInJG shouldBe (0, 0)
      }
    }

    scenario("Storing errors") {
      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig

      withRunningKafka {

        val allRequests: Seq[String] = readAllFiles("/invalid/requests/storing/")
        val allExpectedResults: Seq[String] = readAllFiles("/invalid/expectedResults/storing/")
        val mapReqExpected: Map[String, String] = (allRequests zip allExpectedResults)(breakOut): Map[String, String]

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()

        mapReqExpected.foreach { re =>
          cleanDb()
          publishStringMessageToKafka(topic, re._1)
          Thread.sleep(1000)
          consumeFirstMessageFrom(errorTopic) shouldBe re._2
          howManyElementsInJG shouldBe (0, 0)
        }
      }
    }
  }

  // helpers

  def getDefaultEmbeddedKafkaConfig: EmbeddedKafkaConfig = {
    EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = PortGiver.giveMeZookeeperPort)
  }

  case class ValidTest(request: String, expectedResult: String, nameOfTest: String)

  def getAllValidTests(directory: String): List[ValidTest] = {
    val filesReq: List[File] = getFilesInDirectory(directory + "requests/")
    val filesExpectedResults: List[File] = getFilesInDirectory(directory + "expectedResults/")

    val allExpectedResults: List[(String, String)] = filesExpectedResults map { f =>
      (readFile(f.getCanonicalPath).head, f.getName)
    }

    val allRes: List[String] = filesReq map { f =>
      readFile(f.getCanonicalPath).head
    }
    val res: List[(String, (String, String))] = allRes zip allExpectedResults

    res map { m => ValidTest(m._1, m._2._1, m._2._2) }
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
    GremlinConnector.get
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

}
