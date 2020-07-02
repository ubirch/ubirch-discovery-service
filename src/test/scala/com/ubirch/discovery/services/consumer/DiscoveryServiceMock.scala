package com.ubirch.discovery.services.consumer

import com.typesafe.config.Config
import com.ubirch.discovery.Lifecycle
import com.ubirch.discovery.models.Storer
import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class DiscoveryServiceSendErrorOk @Inject() (storer: Storer, config: Config, lifecycle: Lifecycle)(implicit val ec: ExecutionContext) extends AbstractDiscoveryService(storer, config, lifecycle) with MockitoSugar {

  var counter = 0

  override protected def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = {
    counter = 1
    Future(mock[RecordMetadata])
  }

}
