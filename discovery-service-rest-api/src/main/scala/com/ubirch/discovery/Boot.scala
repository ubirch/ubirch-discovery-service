package com.ubirch.discovery

import java.net.URL

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

object Boot {
  def main(args: Array[String]) {
    val server = new Server(8080)
    val context = new WebAppContext()
    context.setServer(server)
    context.setContextPath("/")
    val confPath: URL = getClass.getResource("/")

    context.setWar(confPath.getPath)
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
