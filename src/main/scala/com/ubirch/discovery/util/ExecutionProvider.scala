package com.ubirch.discovery.util

import java.util.concurrent.Executors

import com.typesafe.config.Config
import javax.inject.{ Inject, Provider, Singleton }
import monix.execution.Scheduler

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }

/**
  * Represents the Execution Context Component used in the system
  */
trait Execution {
  implicit def ec: ExecutionContextExecutor
}

/**
  * Represents the Execution Context provider.
  * Whenever someone injects an ExecutionContext, this provider defines what will
  * be returned.
  * @param config Represents the configuration object
  */
@Singleton
class ExecutionProvider @Inject() (config: Config) extends Provider[ExecutionContext] with Execution {

  val threadPoolSize: Int = config.getInt("core.threads")

  override implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))

  override def get(): ExecutionContext = ec

}

/**
  * Represents a Scheduler used in the system
  */
trait SchedulerBase {
  implicit val scheduler: Scheduler
}

/**
  * Represents the Scheduler Provider
  * @param ec Represents the execution context for async processes.
  */
@Singleton
class SchedulerProvider @Inject() (ec: ExecutionContext) extends Provider[Scheduler] with SchedulerBase {

  override implicit val scheduler: Scheduler = monix.execution.Scheduler(ec)

  override def get(): Scheduler = scheduler

}
