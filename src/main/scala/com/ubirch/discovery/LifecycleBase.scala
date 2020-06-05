package com.ubirch.discovery

import java.util.concurrent.ConcurrentLinkedDeque

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.util.ExecutionContextHelper

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Basic definition for a Life CyCle Component.
  * A component that supports StopHooks
  */
trait Lifecycle {

  def addStopHook(hook: () => Future[_]): Unit

  def stop(): Future[_]

}

object Lifecycle {
  def get: Lifecycle = DefaultLifecycle
}

/**
  * It represents the default implementation for the LifeCycle Component.
  * It actually executes or clears StopHooks.
  */

object DefaultLifecycle
  extends Lifecycle
  with LazyLogging {

  implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  private val hooks = new ConcurrentLinkedDeque[() => Future[_]]()

  override def addStopHook(hook: () => Future[_]): Unit = hooks.push(hook)

  override def stop(): Future[_] = {

    @tailrec
    def clearHooks(previous: Future[Any] = Future.successful[Any](())): Future[Any] = {
      val hook = hooks.poll()
      if (hook != null) clearHooks(previous.flatMap { _ =>
        hook().recover {
          case e => logger.error("Error executing stop hook", e)
        }
      })
      else previous
    }

    logger.info("Running life cycle hooks...")
    clearHooks()
  }
}

/**
  * Definition for JVM ShutDown Hooks.
  */

trait JVMHook {
  protected def registerShutdownHooks(): Unit
}

object JVMHook {
  def get: JVMHook = DefaultJVMHook
}

/**
  * Default Implementation of the JVMHook.
  * It takes LifeCycle stop hooks and adds a corresponding shut down hook.
  */

object DefaultJVMHook extends JVMHook with LazyLogging {

  protected def registerShutdownHooks() {

    def lifecycle: Lifecycle = Lifecycle.get

    logger.info("Registering Shutdown Hooks")

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        lifecycle.stop()

        Thread.sleep(5000) //Waiting 5 secs
        logger.info("Bye bye, see you later...")
      }
    })

  }

  registerShutdownHooks()

}
