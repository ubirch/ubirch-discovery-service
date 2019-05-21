package com.ubirch.discovery

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

object Boot {
  def main(args: Array[String]) {
    val server = new Server(8080)
    val context = new WebAppContext()
    context.setServer(server)
    context.setContextPath("/")
    context.setWar("/Users/benoit/Documents/PoC-rel-managment/ubirch-discovery-service/rest-api/src/main/webapp")
    server.setHandler(context)

    try {
      server.start()
      server.join()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(-1)
    }
  }
}
