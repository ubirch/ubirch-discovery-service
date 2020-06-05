package com.ubirch.discovery.util

import java.util.concurrent.Executors

import com.ubirch.kafka.express.ConfigBase

import scala.concurrent.ExecutionContext

object ExecutionContextHelper extends ConfigBase {

  final val threads = conf.getInt("core.threads")

  final val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threads))

}
