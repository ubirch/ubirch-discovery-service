package com.ubirch.discovery.core.structure

import java.time.{format, ZonedDateTime}
import java.time.format.{DateTimeFormatterBuilder, TextStyle}
import java.util
import java.util.Locale
import java.util.concurrent.ThreadLocalRandom

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.TestUtil
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.util.Util._
import gremlin.scala._
import io.prometheus.client.CollectorRegistry
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.Instant
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers}

import scala.util.Random

class VertexDatabaseSpec extends FeatureSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with LazyLogging {

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

  def warmUpJg(): Option[Vertex] = {
    gc.g.V()
    gc.g.V().valueMap
    gc.g.V().headOption()
  }

  feature("generate a vertex") {

    scenario("test") {
      deleteDatabase()

      val now1: Instant = Instant.now()
      val now = now1.getMillis
      val properties: List[ElementProperty] = List(
        ElementProperty(KeyValue[Any](Number, 6.toLong), PropertyType.Long),
        ElementProperty(KeyValue[Any](Name, "name2"), PropertyType.String),
        ElementProperty(KeyValue[Any](TimeStamp, now), PropertyType.Long)
      )
      implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(properties)
      val vertexInternal = VertexCore(properties, label)
      val vSDb = vertexInternal.toVertexStructDb(gc)

      vSDb.addVertexWithProperties()

      val response: Map[Any, List[Any]] = vSDb.getPropertiesMap
      logger.debug(response.mkString)
      logger.debug(label)

      val propertiesKey = List(Number.name, Name.name, TimeStamp.name, IdAssigned.name)

      val propertiesReceived = recompose(response, propertiesKey)

      // All this trouble is because JansuGraph return the date in his specific way, which needs to be handled.

      import java.text.SimpleDateFormat
      import java.time.ZoneId
      val set = new util.HashSet[ZoneId]()
      set.add(ZoneId.of("Europe/Berlin"))
      val pattern = "MM-dd-yyyy"
      val simpleDateFormat = new SimpleDateFormat(pattern)

      val fmt: format.DateTimeFormatter = new DateTimeFormatterBuilder()
        // your pattern (weekday, month, day, hour/minute/second)
        .appendPattern("EE MMM dd HH:mm:ss ")
        // optional timezone short name (like "CST" or "CEST")
        .optionalStart().appendZoneText(TextStyle.SHORT, set).optionalEnd()
        // optional GMT offset (like "GMT+02:00")
        .optionalStart().appendPattern("OOOO").optionalEnd()
        // year
        .appendPattern(" yyyy")
        // create formatter (using English locale to make sure it parses weekday and month names correctly)
        .toFormatter(Locale.US)

      val correctFormatedReceivedProp = propertiesReceived map {
        p => if (p.keyName == TimeStamp.name) ElementProperty(KeyValue[Any](TimeStamp, ZonedDateTime.parse(p.value.asInstanceOf[String], fmt).toEpochSecond), PropertyType.Long) else p
      }

      val correctFormatedProp = properties map {
        p => if (p.keyName == TimeStamp.name) ElementProperty(KeyValue[Any](TimeStamp, now / 1000L), PropertyType.Long) else p
      }

      logger.info("response: " + response.mkString(", "))
      logger.info("properties received: " + propertiesReceived.mkString(", "))

      correctFormatedReceivedProp.sortBy(x => x.keyName) shouldBe correctFormatedProp.sortBy(x => x.keyName)
    }

  }

  ignore("speed test") {


    def generateProperties: List[ElementProperty] = {
      List(ElementProperty(KeyValue[Any](Number, giveMeRandomLong), PropertyType.Long),
        ElementProperty(KeyValue[Any](Name, giveMeRandomString), PropertyType.String),
        ElementProperty(KeyValue[Any](TimeStamp, Instant.ofEpochSecond(ThreadLocalRandom.current().nextInt()).getMillis), PropertyType.Long))
    }

    scenario("test speed") {
      //deleteDatabase()
      //("speed test not interesting on CI")
      warmUpJg()
      warmUpJg()
      warmUpJg()
      testOld()
      testOld()


      def testOld(): Long = {
        val props = generateProperties
        val vCore = VertexCore(props, "old")
        implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(props)

        val vDb1 = vCore.toVertexStructDb(gc)
        val t1_start: Long = System.currentTimeMillis()
        vDb1.addVertexWithProperties()
        val t1_end = System.currentTimeMillis()
        t1_end - t1_start
      }

      def testNew(): Long = {
        val vCore = VertexCore(generateProperties, "new")
        val t2_start = System.currentTimeMillis()
        var constructor = gc.g.addV(vCore.label)
        for (prop <- vCore.properties) {
          constructor = constructor.property(prop.toKeyValue)
        }
        constructor.iterate()
        val t2_end = System.currentTimeMillis()
        t2_end - t2_start
      }

      var tOld: Long = 0
      var tNew: Long = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")

    }


    scenario("test speed addNewPropertiesToVertex") {
      warmUpJg()
      warmUpJg()
      warmUpJg()
      testOld()
      testOld()

      def testNew(): Long = {
        val vCore = VertexCore(Nil, "new")
        val props = generateProperties
        implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(props)

        val vDb = vCore.toVertexStructDb(gc)
        vDb.addVertexWithProperties()
        val tStart = System.currentTimeMillis()
        var constructor = gc.g.V(vDb.vertex)
        for (prop <- props) {
            constructor = constructor.property(prop.toKeyValue)
          }
        constructor.iterate()
        val tEnd = System.currentTimeMillis()
        tEnd - tStart
      }
      def testOld(): Long = {
        val props = generateProperties
        implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(props)
        val vCore = VertexCore(props, "old")
        val vDb = gc.g.addV("old").l().head
        val tStart = System.currentTimeMillis()
        for (property <- vCore.properties) {
          if (!doesPropExist(property.toKeyValue)) {
            addPropertyToVertex(property.toKeyValue, vDb)
          }
        }

        def addPropertyToVertex[T](property: KeyValue[T], vertex: Vertex) = {
          gc.g.V(vertex).property(property).iterate()
        }

        def doesPropExist[T](keyV: KeyValue[T]): Boolean = gc.g.V(vDb).properties(keyV.key.name).toList().nonEmpty
        val tEnd = System.currentTimeMillis()
        tEnd - tStart
      }

      var tOld: Long = 0
      var tNew: Long = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")

      tOld = 0
      tNew = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")

      tOld = 0
      tNew = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")

      tOld = 0
      tNew = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")

      tOld = 0
      tNew = 0
      for (_ <- 0 to 100) { tOld += testOld(); tNew += testNew() }
      println("current method average time: " + tOld / 100 + " ms")
      println("new method average time: " + tNew / 100 + " ms")
    }

  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
  }

  def giveMeRandomString: String = Random.alphanumeric.take(32).mkString
  def giveMeRandomLong: Long = Random.nextInt().toLong
}

