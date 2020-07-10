package com.ubirch.discovery.models.lock

import java.net.UnknownHostException
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.ConfPaths.RedisConfPaths
import com.ubirch.discovery.Lifecycle
import javax.inject.{ Inject, Singleton }
import monix.execution.Scheduler
import org.redisson.Redisson
import org.redisson.api.{ RedissonClient, RLock, RMapCache }

import scala.concurrent.duration._
import scala.concurrent.Future

/**
  * Cache implementation for redis with a Map that stores the
  * payload/hash as a key and a boolean as the value that is always true
  */
@Singleton
class RedisLock @Inject() (lifecycle: Lifecycle, config: Config)(implicit scheduler: Scheduler)
  extends Lock with LazyLogging with RedisConfPaths {

  private val port: String = config.getString(REDIS_PORT)
  private val password: String = config.getString(REDIS_PASSWORD)
  private val evaluatedPW = if (password == "") null else password
  private val useReplicated: Boolean = config.getBoolean(REDIS_USE_REPLICATED)
  private val cacheName: String = config.getString(REDIS_CACHE_NAME)
  private val cacheTTL: Long = config.getLong(REDIS_CACHE_TTL)
  val redisConf = new org.redisson.config.Config()
  private val prefix = "redis://"

  /**
    * Uses replicated redis server, when used in dev/prod environment.
    */
  if (useReplicated) {
    val mainNode = prefix ++ config.getString(REDIS_MAIN_HOST) ++ ":" ++ port
    val replicatedNode = prefix ++ config.getString(REDIS_REPLICATED_HOST) ++ ":" ++ port
    redisConf.useReplicatedServers().addNodeAddress(mainNode, replicatedNode).setPassword(evaluatedPW)
  } else {
    val singleNode: String = prefix ++ config.getString(REDIS_HOST) ++ ":" ++ port
    redisConf.useSingleServer().setAddress(singleNode).setPassword(evaluatedPW)
  }

  private var redisson: RedissonClient = _

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.seconds

  /**
    * Scheduler trying to connect to redis server with repeating delay if it's not available on startup.
    */
  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redisson = Redisson.create(redisConf)
      stopConnecting()
      logger.info("connection to redis cache has been established.")
    } catch {
      //Todo: should I differentiate? I don't really implement different behaviour till now at least.
      case ex: UnknownHostException =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
      case ex: org.redisson.client.RedisConnectionException =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
      case ex: Exception =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
    }
  }

  private def stopConnecting(): Unit = {
    c.cancel()
  }

  /**
    * Checks if the hash/payload already is stored in the cache.
    *
    * @param hash key
    * @return lock of the hash
    */
  @throws[NoCacheConnectionException]
  def createLock(hash: String): RLock = {
    redisson.getLock("hash:" + hash)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis: " + cacheName)
    Future.successful(redisson.shutdown())
  }

}
