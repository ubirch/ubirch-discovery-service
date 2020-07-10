package com.ubirch.discovery

import com.google.inject.{ AbstractModule, Module }
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.Config
import com.ubirch.discovery.services.consumer.{ AbstractDiscoveryService, DefaultDiscoveryService }
import com.ubirch.discovery.models.{ DefaultJanusgraphStorer, Storer }
import com.ubirch.discovery.models.lock.{ Lock, RedisLock }
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.{ GremlinConnector, JanusGraphConnector }
import com.ubirch.discovery.services.health.HealthChecks
import com.ubirch.discovery.util.{ ExecutionProvider, SchedulerProvider }
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext

class Binder extends AbstractModule {

  def Storer: ScopedBindingBuilder = bind(classOf[Storer]).to(classOf[DefaultJanusgraphStorer])
  def DiscoveryService: ScopedBindingBuilder = bind(classOf[AbstractDiscoveryService]).to(classOf[DefaultDiscoveryService])
  def GremlinConnector: ScopedBindingBuilder = bind(classOf[GremlinConnector]).to(classOf[JanusGraphConnector])
  def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(classOf[ConfigProvider])
  def Lifecycle: ScopedBindingBuilder = bind(classOf[Lifecycle]).to(classOf[DefaultLifecycle])
  def JVMHook: ScopedBindingBuilder = bind(classOf[JVMHook]).to(classOf[DefaultJVMHook])
  def ExecutionContext: ScopedBindingBuilder = bind(classOf[ExecutionContext]).toProvider(classOf[ExecutionProvider])
  def JettyServer: ScopedBindingBuilder = bind(classOf[HealthJettyServer]).to(classOf[DefaultHealthJettyServer])
  def Health: ScopedBindingBuilder = bind(classOf[HealthChecks]).to(classOf[HealthChecks])
  def Lock: ScopedBindingBuilder = bind(classOf[Lock]).to(classOf[RedisLock])
  def Scheduler: ScopedBindingBuilder = bind(classOf[Scheduler]).toProvider(classOf[SchedulerProvider])

  def configure(): Unit = {
    Storer
    DiscoveryService
    GremlinConnector
    Config
    Lifecycle
    JVMHook
    ExecutionContext
    JettyServer
    Lock
    Scheduler
  }

}

object Binder {
  def modules: List[Module] = List(new Binder)
}
