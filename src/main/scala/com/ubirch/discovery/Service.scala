package com.ubirch.discovery

object Service extends Boot(List(new Binder)) {

  def main(args: Array[String]): Unit = {
    get[DiscoverySystem].start()
  }
}
