package com.ubirch.discovery.kafka.util

import com.redis.RedisClient
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.kafka.redis.{ RedisFactory, RedisTypes }
import com.ubirch.kafka.express.ConfigBase

import scala.util.Try

object RedisCache extends ConfigBase with LazyLogging {

  val r: RedisClient = RedisFactory.getInstance(RedisTypes.DefaultRedisClient).r

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

  def isRedisUp: Boolean = r.connected

}
