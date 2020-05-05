package com.ubirch.discovery.kafka.redis

import com.redis.RedisClient

trait RedisConnector {
  val r: RedisClient
  def closeConnection(): Unit
}
