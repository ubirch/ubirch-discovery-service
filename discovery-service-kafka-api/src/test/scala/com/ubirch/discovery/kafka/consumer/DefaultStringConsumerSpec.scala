package com.ubirch.discovery.kafka.consumer

import com.ubirch.discovery.kafka.TestBase
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.io.Source

class DefaultStringConsumerSpec extends TestBase {

  feature("test") {
    scenario("truc") {

      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        kafkaPort = 9092,
        zooKeeperPort = PortGiver.giveMeZookeeperPort
      )

      withRunningKafka {

        val topic = "test"

        val fileTestName = getClass.getResource("/validRequests.txt")
        val source = Source.fromFile(fileTestName.getPath)
        val listRequests = source.getLines.toList
        source.close()

        val messages = listRequests.indices.map(i => { listRequests(i) }).toList
        messages.foreach { m =>
          publishStringMessageToKafka(topic, m)
        }

        // val consumer = DefaultStringConsumer

        // consumer.consumerConfigured.start()

        Thread.sleep(10000)

      }
    }
  }

}
