package com.ubirch.discovery

import java.util.concurrent.CountDownLatch

import com.ubirch.discovery.services.consumer.AbstractDiscoveryService
import javax.inject.{ Inject, Singleton }

@Singleton
class DiscoverySystem @Inject() (discoveryService: AbstractDiscoveryService, jettyServer: HealthJettyServer) {

  def start(): Unit = {
    jettyServer.start()
    discoveryService.start()
    val cd = new CountDownLatch(1)
    cd.await()
  }

}
