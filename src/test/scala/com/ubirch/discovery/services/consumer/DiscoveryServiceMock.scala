package com.ubirch.discovery.services.consumer

import com.typesafe.config.Config
import com.ubirch.discovery.Lifecycle
import com.ubirch.discovery.models.Storer
import com.ubirch.discovery.models.lock.Lock
import monix.execution.Scheduler

import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class DiscoveryServiceSendErrorOk @Inject() (storer: Storer, config: Config, lifecycle: Lifecycle, locker: Lock)(implicit val ec: ExecutionContext) extends AbstractDiscoveryService(storer, config, lifecycle, locker) with MockitoSugar {

  var counter = 0

  override implicit val scheduler: Scheduler = Scheduler(ec)

  override def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = {
    counter = 1
    Future(mock[RecordMetadata])
  }

}
