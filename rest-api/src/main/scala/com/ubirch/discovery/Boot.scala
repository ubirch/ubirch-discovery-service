package com.ubirch.discovery

import java.nio.file.Paths

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

object Boot {
  def main(args: Array[String]) {
    val server = new Server(8080)
    val context = new WebAppContext()
    context.setServer(server)
    context.setContextPath("/")
    val pathWebbApp = Paths.get("rest-api","src", "main", "webapp")
    context.setWar(pathWebbApp.toString)
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
