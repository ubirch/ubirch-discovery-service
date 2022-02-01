package com.ubirch.discovery.models.lock

import java.net.UnknownHostException
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.ConfPaths.RedisConfPaths
import com.ubirch.discovery.Lifecycle

import javax.inject.{ Inject, Singleton }
import monix.execution.Scheduler
import org.redisson.Redisson
import org.redisson.api.{ RLock, RedissonClient }

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

  def createLock(hash: String): Option[RLock] = {
    try {
      Option(redisson.getLock("DiscoveryServiceLockHash:" + hash))
    } catch {
      case _: NullPointerException => None
    }
  }

  def isConnected: Boolean = try {
    redisson.getNodesGroup.pingAll()
  } catch {
    case _: Throwable => false
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis")
    Future.successful(redisson.shutdown())
  }

}
