package com.ubirch.discovery.core

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object ExecutionContextHelper {

  val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

}
