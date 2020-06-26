package com.ubirch.discovery

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.ubirch.discovery.consumer.AbstractDiscoveryService
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.{ GremlinConnector, JanusGraphForTests }
import com.ubirch.discovery.util.RemoteJanusGraph
import com.ubirch.kafka.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import scala.io.Source

//TODO: We need to rethink the tests here are they are causing issues on the ci pipelines
class DefaultStringConsumerSpec extends TestBase {

  val topic = "test"
  val errorTopic = "test.error"
  implicit val Deserializer: StringDeserializer = new StringDeserializer

  /**
    * Simple injector that replaces the kafka bootstrap server and topics to the given ones
    */
  def FakeSimpleInjector(bootstrapServers: String, port: Int = 8183): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, port))
  })) {}

  /**
    * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String, port: Int): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      "core.connector.port",
      ConfigValueFactory.fromAnyRef(port)
    ).withValue(
        "kafkaApi.kafkaConsumer.bootstrapServers",
        ConfigValueFactory.fromAnyRef(bootstrapServers)
      ).withValue(
          "kafkaApi.kafkaProducer.bootstrapServers",
          ConfigValueFactory.fromAnyRef(bootstrapServers)
        )
  }

  RemoteJanusGraph.startJanusGraphServer()

  val Injector = FakeSimpleInjector("")

  feature("Verifying valid requests") {

    def runTest(test: TestStruct): Unit = {

      implicit val gc: GremlinConnector = Injector.get[GremlinConnector]
      cleanDb
      val consumer = Injector.get[AbstractDiscoveryService]
      //cleanDb
      logger.debug("testing " + test.request)
      val crs = new ConsumerRecord[String, String](topic, 0, 79, null, test.request)
      consumer.letsProcess(Vector(crs))
      //val r = consumeFirstStringMessageFrom(topic)
      //println(s"r: $r")
      Thread.sleep(2000)
      howManyElementsInJG shouldBe howManyElementsShouldBeInJg(test.expectedResult)
      //consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)

    }

    val allTests = getAllTests("/valid/")

    //ignore("NeedForJanus") {
    allTests foreach { test =>
      scenario(test.nameOfTest) {
        runTest(test)
      }
    }
    //}

  }

  feature("Invalid requests: Parsing errors") {

    def runTest(test: TestStruct): Unit = {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
      withRunningKafka {

        val Injector = FakeSimpleInjector(bootstrapServers)

        val consumer = Injector.get[AbstractDiscoveryService]
        implicit val gc: GremlinConnector = Injector.get[GremlinConnector]

        consumer.consumption.setForceExit(false)
        consumer.consumption.start()
        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(100)
        consumeFirstMessageFrom(errorTopic) shouldBe test.expectedResult
        consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)
      }
    }

    val allTests = getAllTests("/invalid/parsing/")

    allTests foreach { test =>
      scenario(test.nameOfTest) {
        runTest(test)
      }
    }

  }

  feature("Invalid requests: Storing errors") {

    def runTest(test: TestStruct): Unit = {

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      val Injector = FakeSimpleInjector(bootstrapServers)
      implicit val gc: GremlinConnector = Injector.get[GremlinConnector]
      withRunningKafka {

        cleanDb
        val consumer = Injector.get[AbstractDiscoveryService]
        consumer.consumption.setForceExit(false)
        consumer.consumption.start()

        publishStringMessageToKafka(topic, test.request)
        Thread.sleep(4000)
        consumeFirstMessageFrom(errorTopic) shouldBe test.expectedResult
        howManyElementsInJG shouldBe (0, 0)
        consumer.consumption.shutdown(300, TimeUnit.MILLISECONDS)
      }
    }

    val allTests = getAllTests("/invalid/storing/")

    allTests foreach { test =>
      scenario(test.nameOfTest) {
        runTest(test)
      }
    }

  }

  //   ------ helpers -------

  def getDefaultEmbeddedKafkaConfig: EmbeddedKafkaConfig = {
    EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
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

  def cleanDb(implicit gc: GremlinConnector): Unit = {
    gc.g.V().drop().iterate()
  }

  /**
    * Determine how many elements (vertex and edges) are stored in janusgraph.
    * @return tuple(numberOfVertex: Int, numberOfEdges: Int).
    */
  def howManyElementsInJG(implicit gc: GremlinConnector): (Int, Int) = {
    val numberOfVertices = gc.g.V().count().l().head.toInt
    val numberOfEdges = gc.g.E().count().l().head.toInt
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
