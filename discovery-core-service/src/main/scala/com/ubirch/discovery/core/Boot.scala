package com.ubirch.discovery.core

import com.ubirch.discovery.core.metrics.PrometheusMetrics

trait WithJVMHooks {

  private def bootJVMHook(): JVMHook = JVMHook.get

  bootJVMHook()

}

/**
  * Util that integrates an elegant way to add shut down hooks to the JVM.
  */
trait WithPrometheusMetrics {

  private def bootPrometheusServer() = PrometheusMetrics.get

  bootPrometheusServer()

}

/**
  * Util that is used when starting the main service.
  */
abstract class Boot extends WithJVMHooks with WithPrometheusMetrics

