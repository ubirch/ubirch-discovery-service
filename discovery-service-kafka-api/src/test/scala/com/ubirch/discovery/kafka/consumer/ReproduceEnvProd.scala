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
    "upp-chain",
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
      for (_ <- 1 to 5) {
        val (listInit: Seq[String], keyLeafTree: String, keyDeviceId: String) = initNodes(List(idDevice1, idDevice2, idDevice3))
        listInit.foreach { m =>
          publishStringMessageToKafka(topic, m)
        }

        var lastId = keyDeviceId
        var lastLabel = "signature"
        var counter = 0

        for (_ <- 1 to 50) {

          val dvId = whichDeviceId(Random.nextFloat(), idDevice1, idDevice2, idDevice3)

          if (counter % 2 == 0) {
            val newId = Random.alphanumeric.take(32).mkString
            val newLabel = "generic"
            val req = generateRequest(lastLabel, lastId)(newLabel, newId)
            publishStringMessageToKafka(topic, req)
            lastId = newId
            lastLabel = newLabel
            logger.info("generic")
          } else {
            if (counter % 4 == 1) {
              val newId = Random.alphanumeric.take(32).mkString
              val newLabel = "upp-chain"
              val req1 = generateRequest(lastLabel, lastId)(newLabel, newId)
              publishStringMessageToKafka(topic, req1)
              val req2 = generateRequest(newLabel, newId)("device_id", dvId) //keyDeviceId
              publishStringMessageToKafka(topic, req2)
              val req3 = generateRequest(newLabel, newId)("leaf_tree", keyLeafTree)
              publishStringMessageToKafka(topic, req3)
              lastLabel = newLabel
              lastId = newId
              logger.info("upp-chain")
            } else {
              val newId = Random.alphanumeric.take(32).mkString
              val newLabel = "signature"
              val req1 = generateRequest(lastLabel, lastId)(newLabel, newId)
              publishStringMessageToKafka(topic, req1)
              val req2 = generateRequest(newLabel, newId)("device_id", dvId) //keyDeviceId
              publishStringMessageToKafka(topic, req2)
              val req3 = generateRequest(newLabel, newId)("leaf_tree", keyLeafTree)
              publishStringMessageToKafka(topic, req3)
              lastLabel = newLabel
              lastId = newId
              logger.info("signature")
            }
          }
          counter = counter + 1
          logger.info("counter = " + counter.toString)
          Thread.sleep(250)
        }
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
  def initNodes(listDvId: List[String]): (List[String], String, String) = {
    val listId = List(
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString,
      Random.alphanumeric.take(32).mkString
    )
    val initMsg = List(
      generateRequest("blockchain_ETH", listId(0))("root_tree", listId(1)),
      generateRequest("root_tree", listId(1))("blockchain_IOTA", listId(2)),
      generateRequest("leaf_tree", listId(3))("root_tree", listId(1)),
      generateRequest("leaf_tree", listId(3))("signature", listId(4)),
      generateRequest("device_id", listDvId(0))("signature", listId(4)),
      generateRequest("leaf_tree", listId(3))("signature", listId(5)),
      generateRequest("device_id", listDvId(1))("signature", listId(5)),
      generateRequest("leaf_tree", listId(3))("signature", listId(6)),
      generateRequest("device_id", listDvId(2))("signature", listId(6))
    )
    (initMsg, listId(3), listId(5))
  }

  def generateRequest(tn1: String, k1: String)(tn2: String, k2: String): String = {
    val v1 = generateVertex(tn1, k1, "v1")
    val v2 = generateVertex(tn2, k2, "v2")
    val edge = generateEdge()
    val req = s"""[{$v1,$v2,$edge}]"""
    //    logger.info("*req: " + req)
    req
  }

  /*
    * Generate a vertex that has the following structure:
    * "v1Orv2":{"id":"typeNode", "properties":{"timeStamp":"CREATION_TIME}, "label":"TYPE_NODE"}
    */
  def generateVertex(typeNode: String, key: String, v1Orv2: String): String = {
    val properties = generateProperties()
    val label = generateLabel(typeNode)
    val id = generateId(key)
    val vertex = s"""\"$v1Orv2\":{$id,$properties,$label}"""
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
    val properties = generateProperties()
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
  def generateProperties(): String = {
    val timestamp: String = (System.currentTimeMillis / 100).toString
    val prop = s"""\"properties\":{\"timeStamp\":\"$timestamp\"}"""
    //    logger.info("props: " + prop)
    prop
  }

  def getListMsg(accu: List[String], counter: Int): List[String] = {
    counter match {
      case 0 => accu
      case _ => getListMsg(generateRequest(listLabelsVertex(counter % 5), counter.toString)(listLabelsVertex((counter + 2) % 5), (counter * 100).toString) :: accu, counter - 1)
    }
  }
}

