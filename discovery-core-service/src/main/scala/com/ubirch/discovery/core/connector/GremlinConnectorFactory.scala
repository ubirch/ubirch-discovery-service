package com.ubirch.discovery.core.connector

import com.typesafe.config.Config
import com.ubirch.discovery.core.connector.ConnectorType.ConnectorType
import org.apache.commons.configuration.PropertiesConfiguration

import scala.collection.JavaConverters._

object GremlinConnectorFactory {

  private lazy val instanceTest = new JanusGraphForTests
  private lazy val instanceJanusGraph = new JanusGraphConnector

  def getInstance(connectorType: ConnectorType): GremlinConnector = {
    connectorType match {
      case ConnectorType.JanusGraph => instanceJanusGraph
      case ConnectorType.Test => instanceTest
    }
  }

  def buildProperties(config: Config): PropertiesConfiguration = {
    val conf = new PropertiesConfiguration()

    val hosts: List[String] = config.getString("core.connector.hosts")
      .split(",")
      .toList
      .map(_.trim)
      .filter(_.nonEmpty)

    conf.addProperty("hosts", hosts.asJava)
    conf.addProperty("port", config.getString("core.connector.port"))
    conf.addProperty("serializer.className", config.getString("core.connector.serializer.className"))
    // no idea why the following line needs to be duplicated. Doesn't work without
    // cf https://stackoverflow.com/questions/45673861/how-can-i-remotely-connect-to-a-janusgraph-server first answer, second comment ¯\_ツ_/¯
    conf.addProperty("serializer.config.ioRegistries", config.getAnyRef("core.connector.serializer.config.ioRegistries").asInstanceOf[java.util.ArrayList[String]])
    conf.addProperty("serializer.config.ioRegistries", config.getStringList("core.connector.serializer.config.ioRegistries"))

    val maxWaitForConnection = config.getInt("core.connector.connectionPool.maxWaitForConnection")
    if(maxWaitForConnection > 0) conf.addProperty("connectionPool.maxWaitForConnection", maxWaitForConnection)

    val reconnectInterval = config.getInt("core.connector.connectionPool.reconnectInterval")
    if(reconnectInterval > 0) conf.addProperty("connectionPool.reconnectInterval", reconnectInterval)

    val connectionMinSize = config.getInt("core.connector.connectionPool.minSize")
    if(connectionMinSize > 0) conf.addProperty("connectionPool.minSize", connectionMinSize)

    val connectionMaxSize = config.getInt("core.connector.connectionPool.maxSize")
    if(connectionMaxSize > 0) conf.addProperty("connectionPool.maxSize", connectionMaxSize)

    val nioPoolSize = config.getInt("core.connector.nioPoolSize")
    if(nioPoolSize > 0) conf.addProperty("nioPoolSize", nioPoolSize)

    val workerPoolSize = config.getInt("core.connector.workerPoolSize")
    if(workerPoolSize > 0) conf.addProperty("workerPoolSize", workerPoolSize)

    conf
  }
}
