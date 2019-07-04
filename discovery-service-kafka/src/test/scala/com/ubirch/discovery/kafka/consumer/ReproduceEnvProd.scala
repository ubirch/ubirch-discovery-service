package com.ubirch.discovery.kafka.consumer

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.kafka.TestBase
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.EmbeddedKafkaConfig

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
    "signature",
    "upp",
    "generic",
    "device_id"
  )

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = PortGiver.giveMeZookeeperPort
  )

  def main(args: Array[String]): Unit = {

    withRunningKafka {

      val topic = "test"
      val consumer = new DefaultExpressDiscoveryApp {}

      val idDevice1 = Random.alphanumeric.take(32).mkString
      val idDevice2 = Random.alphanumeric.take(32).mkString
      val idDevice3 = Random.alphanumeric.take(32).mkString

      consumer.consumption.start()
      var counterRT = 0
      for (_ <- 1 to 5) {
        val (listInit: Seq[String], keyRootTree: String) = initNodes()
        listInit.foreach { m =>
          publishStringMessageToKafka(topic, m)
        }

        val sigOrigUpp = generateNewKey
        val reqOrigUppDID = generateRequest("upp", sigOrigUpp)("device_id", whichDeviceId(Random.nextFloat(), idDevice1, idDevice2, idDevice3))
        publishStringMessageToKafka(topic, reqOrigUppDID)

        var counterFT = 0
        for (i <- 1 to 3) {
          val keyFoundationTree = generateNewKey
          val foundationToRoot = generateRequest("root_tree", keyRootTree)("foundation_tree", keyFoundationTree)
          publishStringMessageToKafka(topic, foundationToRoot)
          var signature = if (i == 1) sigOrigUpp else generateNewKey

          if (i == 1) {
            val uppToFt = generateRequest("upp", signature)("foundation_tree", keyFoundationTree)
            publishStringMessageToKafka(topic, uppToFt)
          }

          var counterUPP = 0

          for (_ <- 1 to 10) {

            val dvId = whichDeviceId(Random.nextFloat(), idDevice1, idDevice2, idDevice3)

            val chain = signature
            signature = generateNewKey

            val uppToFt = generateRequest("upp", signature)("foundation_tree", keyFoundationTree)
            publishStringMessageToKafka(topic, uppToFt)

            //            val uppToSign = generateRequest("upp", chain)("signature", idSign)
            //            publishStringMessageToKafka(topic, uppToSign)

            val uppToDevId = generateRequest("upp", signature)("device_id", dvId)
            publishStringMessageToKafka(topic, uppToDevId)

            val uppToOldUpp = generateRequest("upp", signature)("upp", chain)
            publishStringMessageToKafka(topic, uppToOldUpp)

            counterUPP = counterUPP + 1
            logger.info("counterRT = " + counterRT + ", counterFT = " + counterFT.toString + ", counterUPP = " + counterUPP.toString)
            Thread.sleep(250)
          }
          counterFT = counterFT + 1
        }
        counterRT = counterRT + 1
      }
      Thread.sleep(30000)
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
    val initMsg = List(
      generateRequest("blockchain_ETH", listId.head)("root_tree", listId(1)), //1
      generateRequest("root_tree", listId(1))("blockchain_IOTA", listId(2)) //2
    )
    (initMsg, listId(1))
  }

  def generateRequest(tn1: String, k1: String)(tn2: String, k2: String): String = {
    val v1 = generateVertex(tn1, k1, "v1")
    val v2 = generateVertex(tn2, k2, "v2")
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
  def generateVertex(typeNode: String, key: String, v1Orv2: String): String = {
    val properties = generatePropertiesVertex(key)
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

}

