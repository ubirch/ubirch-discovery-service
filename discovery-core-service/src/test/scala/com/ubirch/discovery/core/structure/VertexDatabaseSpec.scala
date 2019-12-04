package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.TestUtil
import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.util.Util._
import gremlin.scala._
import io.prometheus.client.CollectorRegistry
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers }

class VertexDatabaseSpec extends FeatureSpec
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  private val dateTimeFormat = ISODateTimeFormat.dateTime()
  val label = "aLabel"
  val Number: Key[Any] = Key[Any]("number")
  val Name: Key[Any] = Key[Any]("name")
  val TimeStamp: Key[Any] = Key[Any]("timestamp")
  val Test: Key[Any] = Key[Any]("truc")
  val IdAssigned: Key[Any] = Key[Any]("IdAssigned")

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  feature("generate a vertex") {

    scenario("test") {
      deleteDatabase()

      val now = System.currentTimeMillis
      val properties: List[ElementProperty] = List(
        ElementProperty(KeyValue[Any](Number, 6.toLong), PropertyType.Long),
        ElementProperty(KeyValue[Any](Name, "name2"), PropertyType.String),
        ElementProperty(KeyValue[Any](TimeStamp, now), PropertyType.Long)
      )
      implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(properties)
      val vertexInternal = VertexCore(properties, label)
      val vSDb = vertexInternal.toVertexStructDb(gc)

      vSDb.addVertexWithProperties()

      val response = vSDb.getPropertiesMap
      logger.debug(response.mkString)
      logger.debug(label)

      val propertiesKey = List(Number.name, Name.name, TimeStamp.name, IdAssigned.name)

      val propertiesReceived = recompose(response, propertiesKey)

      logger.info("response: " + response.mkString(", "))
      logger.info("properties received: " + propertiesReceived.mkString(", "))

      propertiesReceived.sortBy(x => x.keyName) shouldBe properties.sortBy(x => x.keyName)
    }

  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
  }

}

