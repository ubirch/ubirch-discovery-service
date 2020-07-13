package com.ubirch.discovery.models.lock

import org.redisson.api.RLock

case class NoCacheConnectionException(
    private val message: String = "",
    private val cause: Throwable = None.orNull
) extends Exception(message, cause)

trait Lock {
  def createLock(hash: String): Option[RLock]
}
