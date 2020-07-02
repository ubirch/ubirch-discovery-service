package com.ubirch.discovery

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import javax.inject.{ Inject, Singleton }
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import scala.concurrent.Future

trait HealthJettyServer {
  def start(): Unit
}

@Singleton
class DefaultHealthJettyServer @Inject() (conf: Config, lifecycle: Lifecycle) extends HealthJettyServer with LazyLogging {

  val server = new Server(conf.getInt("healthcheck.port"))

  // context for main scalatra rest API
  val scalatraContext: WebAppContext = new WebAppContext()
  scalatraContext.setContextPath("/")
  scalatraContext.setResourceBase("src/main/scala")
  scalatraContext.addEventListener(new ScalatraListener)
  scalatraContext.addServlet(classOf[DefaultServlet], "/")
  scalatraContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false")

  // context for swagger-ui

  val contexts = new ContextHandlerCollection()
  contexts.setHandlers(Array(scalatraContext))
  server.setHandler(contexts)

  def start() = {
    try {
      server.start()
      //server.join()
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        System.exit(-1)
    }
    addShutdownHook(server)
  }

  private def addShutdownHook(server: Server) = {
    lifecycle.addStopHook { () =>
      logger.info("Shutting down Restful Service")
      Future.successful(server.stop())
    }
  }

}
