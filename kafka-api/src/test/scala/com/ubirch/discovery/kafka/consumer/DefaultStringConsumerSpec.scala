package com.ubirch.discovery.kafka.consumer

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.kafka.TestBase
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.io.Source

class DefaultStringConsumerSpec extends TestBase {

  feature("test") {
    scenario("truc") {
      val maxEntities = 2
      val counter = new CountDownLatch(maxEntities)

      //val confDefault: Config = ConfigFactory.load("application.base.conf")

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

        val messages = (0 to maxEntities).map(i => { listRequests(i) }).toList
        messages.foreach { m =>
          publishStringMessageToKafka(topic, m)
        }

        val consumer = DefaultStringConsumer

        consumer.consumerConfigured.start()

        Thread.sleep(5000)

      }
    }
  }

}
