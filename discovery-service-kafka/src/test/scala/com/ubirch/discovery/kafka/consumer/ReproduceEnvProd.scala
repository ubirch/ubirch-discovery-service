package com.ubirch.discovery.kafka.consumer

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.kafka.TestBase
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.collection.immutable
import scala.util.Random

object ReproduceEnvProd extends TestBase with LazyLogging {

  val listSign = new scala.collection.mutable.ListBuffer[String]
  val listBx = new scala.collection.mutable.ListBuffer[String]

  val listLabelsEdge = List(
    "transaction",
    "link",
    "associate",
    "father",
    "generate"
  )

  val listLabelsVertex = List(
    "blockchain_IOTA",
    "blockchain_ETH",
    "root_tree",
    "leaf_tree",
    "upp",
    "device_id"
  )

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9093,
    zooKeeperPort = PortGiver.giveMeZookeeperPort
  )

  def main(args: Array[String]): Unit = {

    withRunningKafka {

      logger.info(config.kafkaPort.toString)
      val topic = "test"
      logger.info("starting consumer")
      val consumer = new DefaultExpressDiscoveryApp {}
      consumer.consumption.start()
      logger.info("consumer started")

      val idDevice1 = Random.alphanumeric.take(32).mkString
      val idDevice2 = Random.alphanumeric.take(32).mkString
      val idDevice3 = Random.alphanumeric.take(32).mkString

      var counterRT = 0
      for (_ <- 1 to 30) {
        val (listInit: Seq[String], keyRootTree: String) = initNodes()
        listInit.foreach { m =>
          logger.info(m)
          publishStringMessageToKafka(topic, m)
        }

        val sigOrigUpp = generateNewKey
        val reqOrigUppDID = generateRequest("upp", Map("signature" -> sigOrigUpp, "type" -> "upp"))("device_id", Map("device-id" -> whichDeviceId(Random.nextFloat(), idDevice1, idDevice2, idDevice3), "type" -> "device-id"))
        publishStringMessageToKafka(topic, reqOrigUppDID)

        var counterFT = 0
        for (i <- 1 to 3) {
          val keyFoundationTree = generateNewKey
          val foundationToRoot = generateRequest("root_tree", Map("hash" -> keyRootTree, "type" -> "root_tree"))("foundation_tree", Map("hash" -> keyFoundationTree, "type" -> "foundation_tree"))
          publishStringMessageToKafka(topic, foundationToRoot)
          var signature = if (i == 1) sigOrigUpp else generateNewKey

          if (i == 1) {
            val uppToFt = generateRequest("upp", Map("signature" -> signature, "type" -> "upp"))("foundation_tree", Map("hash" -> keyFoundationTree, "type" -> "foundation_tree"))
            publishStringMessageToKafka(topic, uppToFt)
          }

          var counterUPP = 0

          for (_ <- 1 to 30) {

            val dvId = whichDeviceId(Random.nextFloat(), idDevice1, idDevice2, idDevice3)

            val chain = signature
            signature = generateNewKey

            val uppToFt = generateRequest("upp", Map("signature" -> signature, "type" -> "upp"))("foundation_tree", Map("hash" -> keyFoundationTree, "type" -> "foundation_tree"))
            publishStringMessageToKafka(topic, uppToFt)

            val uppToDevId = generateRequest("upp", Map("signature" -> signature, "type" -> "upp"))("device_id", Map("device-id" -> dvId, "type" -> "device-id"))
            publishStringMessageToKafka(topic, uppToDevId)

            val uppToOldUpp = generateRequest("upp", Map("signature" -> signature, "type" -> "upp"))("upp", Map("signature" -> chain, "type" -> "upp"))
            publishStringMessageToKafka(topic, uppToOldUpp)

            counterUPP = counterUPP + 1
            //logger.info("counterRT = " + counterRT + ", counterFT = " + counterFT.toString + ", counterUPP = " + counterUPP.toString)
            Thread.sleep(10)
          }
          counterFT = counterFT + 1
        }
        counterRT = counterRT + 1
      }
      Thread.sleep(50)
    }
  }

  def whichDeviceId(rnd: Float, k1: String, k2: String, k3: String): String = {
    rnd match {
      case x if x <= 0.1 => k1
      case x if x >= 0.4 => k3
      case _ => k2
    }
  }

  /**
    * @return (List[String], String, String)
    *         1/: list of messages to be sent
    *         2/: key leaf tree
    *         3/: key device id
    */
  def initNodes(): (List[String], String) = {
    val listId = List(
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString
    )

    val propertiesBcxEth = Map("hash" -> listId.head, "type" -> "blockchain_ETH")
    val propertiesBcxIOTA = Map("hash" -> listId(2), "type" -> "blockchain_IOTA")
    val propertiesRootTree = Map("hash" -> listId(1))
    val initMsg = List(
      generateRequest("blockchain", propertiesBcxEth)("root_tree", propertiesRootTree), //1
      generateRequest("blockchain", propertiesBcxIOTA)("root_tree", propertiesRootTree) //2
    )
    (initMsg, listId(1))
  }

  def generateRequest(tn1: String, p1: Map[String, String])(tn2: String, p2: Map[String, String]): String = {
    val v1 = generateVertex(tn1, p1, "v1")
    val v2 = generateVertex(tn2, p2, "v2")
    val edge = generateEdge()
    val req = s"""[{$v1,$v2,$edge}]"""
    //    logger.info("*req: " + req)
    req
  }

  def generateNewKey: String = Random.alphanumeric.take(32).mkString

  /*
    * Generate a vertex that has the following structure:
    * "v1Orv2":{"id":"typeNode", "properties":{"timeStamp":"CREATION_TIME}, "label":"TYPE_NODE"}
    */
  def generateVertex(typeNode: String, props: Map[String, String], v1Orv2: String): String = {
    val properties = generatePropertiesVertex(props)
    val label = generateLabel(typeNode)
    val vertex = s"""\"$v1Orv2\":{$properties,$label}"""
    //    logger.info("vertex: " + vertex)
    vertex
  }

  def generateId(key: String): String = {
    val id: String = s"""\"id\":\"$key\""""
    //    logger.info("id: " + id)
    id
  }

  // format: {"edge":{"properties":{"timeStamp":"TIME_CREATION"}}}
  def generateEdge(): String = {
    val properties = generatePropertiesEdge()
    val edge = s"""\"edge\":{$properties}"""
    //    logger.info("edge: " + edge)
    edge
  }

  def generateLabel(lbl: String): String = {
    val label = s"""\"label\":\"$lbl\""""
    //    logger.info("label: " + label)
    label
  }

  /**
    * Generate a property string, contains a timestamp of its creation
    *
    * @return a string having the following structure :
    *         "properties":{"timeStamp":"TIME"}
    */
  def generatePropertiesEdge(): String = {
    val timestamp: String = (System.currentTimeMillis / 10).toString
    val prop = s"""\"properties\":{\"timeStamp\":\"$timestamp\"}"""
    //    logger.info("props: " + prop)
    prop
  }

  def generatePropertiesVertex(signature: String): String = {
    val prop = s"""\"properties\":{\"signature\":\"$signature\"}"""
    //    logger.info("props: " + prop)
    prop
  }

  def generatePropertiesVertex(lProps: Map[String, String]): String = {
    val prop: immutable.Iterable[String] = lProps map { kv => s"""\"${kv._1}\":\"${kv._2}\",""" }
    val header = s"""\"properties\":{"""
    val bottom = s"""}"""
    val res = header + prop.mkString("").dropRight(1) + bottom
    //s"""\"properties\":{\"signature\":\"$signature\"}"""
    res
  }

}

