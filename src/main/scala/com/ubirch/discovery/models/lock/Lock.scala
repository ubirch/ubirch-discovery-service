package com.ubirch.discovery.models.lock

import org.redisson.api.RLock

trait Lock {

  /**
    * Checks if the hash/payload already is stored in the cache.
    *
    * @param hash key
    * @return lock of the hash
    */
  def createLock(hash: String): Option[RLock]

  /**
    * @return True if the lock is successfully connected to redis. False otherwise.
    */
  def isConnected: Boolean
}
