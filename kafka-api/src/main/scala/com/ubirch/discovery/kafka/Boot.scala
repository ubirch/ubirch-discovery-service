package com.ubirch.discovery.kafka


trait WithJVMHooks {

  private def bootJVMHook(): JVMHook = DefaultJVMHook

  bootJVMHook()

}


/**
  * Util that is used when starting the main service.
  */
abstract class Boot extends WithJVMHooks


