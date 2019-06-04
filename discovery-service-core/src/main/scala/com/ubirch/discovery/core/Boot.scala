package com.ubirch.discovery.core

trait WithJVMHooks {

  private def bootJVMHook(): JVMHook = JVMHook.get

  bootJVMHook()

}

/**
  * Util that is used when starting the main service.
  */
abstract class Boot extends WithJVMHooks

