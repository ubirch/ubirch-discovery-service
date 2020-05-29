package com.ubirch.discovery.kafka.redis

import com.ubirch.discovery.kafka.redis.RedisTypes.RedisType

object RedisFactory {

  private lazy val redisDefaultConnector = new DefaultRedisClient

  def getInstance(redisType: RedisType): DefaultRedisClient = {
    redisType match {
      case RedisTypes.Test => redisDefaultConnector
      case RedisTypes.DefaultRedisClient => redisDefaultConnector
    }
  }

}

object RedisTypes extends Enumeration {
  type RedisType = Value
  val Test, DefaultRedisClient = Value
}
