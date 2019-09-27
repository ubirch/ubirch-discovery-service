package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.util.Util._
import gremlin.scala._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{FeatureSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class VertexServerSpec extends FeatureSpec with Matchers with LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  private val dateTimeFormat = ISODateTimeFormat.dateTime()
  val label = "aLabel"
  val Number: Key[String] = Key[String]("number")
  val Name: Key[String] = Key[String]("name")
  val Created: Key[String] = Key[String]("created")
  val test: Key[String] = Key[String]("truc")
  val IdAssigned: Key[String] = Key[String]("IdAssigned")

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  feature("generate a vertex") {

    scenario("test") {
      deleteDatabase()

      val now = DateTime.now(DateTimeZone.UTC)
      val properties: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "5"),
        new KeyValue[String](Name, "aName"),
        new KeyValue[String](Created, dateTimeFormat.print(now))
      )
      implicit val propSet: Set[Elements.Property] = putPropsOnPropSet(properties)

      val vertexInternal = VertexCore(properties, label)
      val vSDb = vertexInternal.toVertexStructDb(gc.g)

      vSDb.addVertexWithProperties(gc.b)

      val response = vSDb.getPropertiesMap
      logger.debug(response.mkString)
      logger.debug(label)

      val propertiesKey = Array(Number, Name, Created, IdAssigned)

      val propertiesReceived = recompose(response, propertiesKey)

      logger.info("respomnse: " + response.mkString(", "))
      logger.info("properties received: " + propertiesReceived.mkString(", "))

      propertiesReceived.sortBy(x => x.key.name) shouldBe properties.sortBy(x => x.key.name)
    }

  }

  def putPropsOnPropSet(propList: List[KeyValue[String]]): Set[Elements.Property] = {
    def iterateOnListProp(it: List[KeyValue[String]], accu: Set[Elements.Property]): Set[Elements.Property] = {
      it match {
        case Nil => accu
        case x :: xs => iterateOnListProp(xs, accu ++ Set(new Elements.Property(x.key.name, true)))
      }
    }

    iterateOnListProp(propList, Set())
  }
}

