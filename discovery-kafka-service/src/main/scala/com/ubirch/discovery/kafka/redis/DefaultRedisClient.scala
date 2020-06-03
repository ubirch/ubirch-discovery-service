package com.ubirch.discovery.kafka.redis

import com.redis.RedisClient
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.Lifecycle
import com.ubirch.kafka.express.ConfigBase

import scala.concurrent.Future

class DefaultRedisClient extends RedisConnector with LazyLogging {

  private def conf = ConfigBase.conf

  val r: RedisClient = {
    new RedisClient(
      conf.getString("redis.address"),
      conf.getInt("redis.port")
    )
  }

  if (!r.auth(conf.getString("redis.password"))) {
    logger.error("Could not authenticate against redis")
  }

  def closeConnection(): Unit = {
    r.close()
  }

  Lifecycle.get.addStopHook { () =>
    logger.info("Shutting down connection with Redis: " + r.toString)
    Future.successful(closeConnection())
  }

}
