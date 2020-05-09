package com.ubirch.discovery.kafka.util

import com.redis.RedisClientPool
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.kafka.redis.{ RedisFactory, RedisTypes }
import com.ubirch.kafka.express.ConfigBase

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success }
import scala.concurrent.duration._

object RedisCache extends ConfigBase with LazyLogging {

  val r = RedisFactory.getInstance(RedisTypes.DefaultRedisClient).r

  def hgetall(hash: String): Option[Map[String, String]] = {
    r.hgetall("vHash:" + hash)
  }

  def getAllFromHash(hash: String): Option[Map[String, String]] = {
    r.hgetall("vHash:" + hash)
  }

  def updateVertex(hash: String, values: Map[String, String]): Boolean = {
    r.hmset("vHash:" + hash, values)
    r.expire("vHash:" + hash, 600)
  }

  def flow[A](noOfRecipients: Int, opsPerClient: Int, keyPrefix: String,
      fn: (Int, String) => A)(implicit ec: ExecutionContext) = {
    (1 to noOfRecipients) map { i =>
      Future {
        fn(opsPerClient, keyPrefix + i)
      }
    }
  }

}
