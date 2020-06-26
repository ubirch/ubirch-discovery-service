package com.ubirch.discovery

import com.google.inject.{ AbstractModule, Module }
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.Config
import com.ubirch.discovery.consumer.{ AbstractDiscoveryService, DefaultDiscoveryService }
import com.ubirch.discovery.process.{ DefaultJanusgraphStorer, Storer }
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.{ GremlinConnector, JanusGraphConnector }
import com.ubirch.discovery.util.ExecutionProvider

import scala.concurrent.ExecutionContext

class Binder extends AbstractModule {

  def Storer: ScopedBindingBuilder = bind(classOf[Storer]).to(classOf[DefaultJanusgraphStorer])
  def DiscoveryService: ScopedBindingBuilder = bind(classOf[AbstractDiscoveryService]).to(classOf[DefaultDiscoveryService])
  def GremlinConnector: ScopedBindingBuilder = bind(classOf[GremlinConnector]).to(classOf[JanusGraphConnector])
  def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(classOf[ConfigProvider])
  def Lifecycle: ScopedBindingBuilder = bind(classOf[Lifecycle]).to(classOf[DefaultLifecycle])
  def JVMHook: ScopedBindingBuilder = bind(classOf[JVMHook]).to(classOf[DefaultJVMHook])
  def ExecutionContext: ScopedBindingBuilder = bind(classOf[ExecutionContext]).toProvider(classOf[ExecutionProvider])

  def configure(): Unit = {
    Storer
    DiscoveryService
    GremlinConnector
    Config
    Lifecycle
    JVMHook
    ExecutionContext
  }

}

object Binder {
  def modules: List[Module] = List(new Binder)
}
