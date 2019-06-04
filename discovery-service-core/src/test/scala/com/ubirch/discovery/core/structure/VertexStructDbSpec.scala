package com.ubirch.discovery.core.structure

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.util.Util._
import gremlin.scala._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FeatureSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class VertexStructDbSpec extends FeatureSpec with Matchers {

  implicit val gc: GremlinConnector = new GremlinConnector

  private val dateTimeFormat = ISODateTimeFormat.dateTime()

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

      val theId = 1

      val vSDb = new VertexStructDb(theId.toString, gc.g)
      val now = DateTime.now(DateTimeZone.UTC)
      val properties: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "5"),
        new KeyValue[String](Name, "aName"),
        new KeyValue[String](Created, dateTimeFormat.print(now))
      )
      vSDb.addVertex(properties, "aLabel", gc.b)

      val response = vSDb.getPropertiesMap
      val label = vSDb.vertex.label
      log.info(response.mkString)
      log.info(label)

      val propertiesKey = Array(Number, Name, Created, IdAssigned)

      val idGottenBack = extractValue[String](response, "IdAssigned")
      val propertiesReceived = recompose(response, propertiesKey)

      propertiesReceived.sortBy(x => x.key.name) shouldBe properties.sortBy(x => x.key.name)
      idGottenBack shouldBe theId.toString
    }

  }

}

