package com.ubirch.discovery.kafka

import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait TestBase
  extends FeatureSpec
  with ScalaFutures
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with Matchers
  with LazyLogging
  with EmbeddedKafka {

  def await[T](future: Future[T]): T = await(future, Duration.Inf)

  def await[T](future: Future[T], atMost: Duration): T = Await.result(future, atMost)

}
