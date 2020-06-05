package com.ubirch.discovery

import com.ubirch.discovery.services.metrics.PrometheusMetrics

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

