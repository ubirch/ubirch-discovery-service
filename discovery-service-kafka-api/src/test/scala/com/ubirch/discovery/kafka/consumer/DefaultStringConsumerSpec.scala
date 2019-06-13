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

  feature("test") {
    scenario("truc") {
      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      cleanDb()
      withRunningKafka {
        val listRequests = readFile("/validRequests.txt")

        listRequests.foreach { m =>
          publishStringMessageToKafka(topic, m)
        }

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()

        Thread.sleep(10000)
        howManyElementsInJG shouldBe (8, 4)
      }
    }
  }

  feature("Verifying invalid requests") {
    scenario("Parsing errors") {
      implicit val config: EmbeddedKafkaConfig = getDefaultEmbeddedKafkaConfig
      cleanDb()
      withRunningKafka {
        logger.info("putain")
        val allRequests: Seq[String] = readAllFiles("/invalid/requests/parsing/")
        val allErrors: Seq[String] = readAllFiles("/invalid/errorMessages/parsing/")
        val mapReqErr: Map[String, String] = (allRequests zip allErrors)(breakOut): Map[String, String]

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()

        mapReqErr.foreach { re =>
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
        val allErrors: Seq[String] = readAllFiles("/invalid/errorMessages/storing/")
        val mapReqErr: Map[String, String] = (allRequests zip allErrors)(breakOut): Map[String, String]

        val consumer = new DefaultExpressDiscoveryApp {}
        consumer.consumption.start()

        mapReqErr.foreach { re =>
          cleanDb()
          publishStringMessageToKafka(topic, re._1)
          Thread.sleep(1000)
          consumeFirstMessageFrom(errorTopic) shouldBe re._2
          howManyElementsInJG shouldBe (0, 0)
        }


      }
    }
  }

  def getDefaultEmbeddedKafkaConfig: EmbeddedKafkaConfig = {
    EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = PortGiver.giveMeZookeeperPort)
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

}
