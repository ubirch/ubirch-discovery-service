package com.ubirch.discovery

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.consumer.AbstractDiscoveryService
import javax.inject.{ Inject, Singleton }

@Singleton
class DiscoverySystem @Inject() (discoveryService: AbstractDiscoveryService) {

  def start(): Unit = {
    discoveryService.start()
    val cd = new CountDownLatch(1)
    cd.await()
  }

}
