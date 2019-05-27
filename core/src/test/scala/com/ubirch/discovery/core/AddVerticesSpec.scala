package com.ubirch.discovery.core

import com.ubirch.discovery.core.Util.{extractValue, getEdge, getEdgeProperties, recompose}
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.VertexStructDb
import gremlin.scala._
import org.apache.tinkerpop.gremlin.structure.Edge
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FeatureSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class AddVerticesSpec extends FeatureSpec with Matchers {

  implicit val gc: GremlinConnector = new GremlinConnector

  private val dateTimeFormat = ISODateTimeFormat.dateTime()

  val Number: Key[String] = Key[String]("number")
  val Name: Key[String] = Key[String]("name")
  val Created: Key[String] = Key[String]("created")
  val IdAssigned: Key[String] = Key[String]("IdAssigned")
  implicit val ordering: (KeyValue[String] => String) => Ordering[KeyValue[String]] = Ordering.by[KeyValue[String], String](_)

  def log: Logger = LoggerFactory.getLogger(this.getClass)

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  feature("add vertices") {
    scenario("add two unlinked vertex") {
      // clean database
      deleteDatabase()

      // prepare
      val id1 = 1
      val id2 = 2

      val now1 = DateTime.now(DateTimeZone.UTC)
      val p1: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "5"),
        new KeyValue[String](Name, "aName1"),
        new KeyValue[String](Created, dateTimeFormat.print(now1))
      )
      val now2 = DateTime.now(DateTimeZone.UTC)
      val p2: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "6"),
        new KeyValue[String](Name, "aName2"),
        new KeyValue[String](Created, dateTimeFormat.print(now2))
      )
      val pE: List[KeyValue[String]] = List(
        new KeyValue[String](Name, "edge")
      )

      // commit
      new AddVertices().addTwoVertices(id1.toString, p1)(id2.toString, p2)(pE)

      // analyse
      //    count number of vertices and edges
      val nbVertices = gc.g.V().count().toSet().head
      val nbEdges = gc.g.E.count().toSet().head
      nbVertices shouldBe 2
      nbEdges shouldBe 1

      //    edges
      val edge1: Edge = getEdge(gc, id1.toString, id2.toString, IdAssigned)

      val arrayKeysE = Array(Name, Number)

      val edgeReceived1 = recompose(getEdgeProperties(gc, edge1), arrayKeysE)

      edgeReceived1.sortBy(x => x.key.name) shouldBe pE.sortBy(x => x.key.name)

      //    vertices
      val v1Reconstructed = new VertexStructDb(id1.toString, gc.g)
      val v2Reconstructed = new VertexStructDb(id2.toString, gc.g)

      val arrayKeysV = Array(IdAssigned, Name, Created, Number)

      val response1 = v1Reconstructed.getPropertiesMap
      val idGottenBack1 = extractValue[String](response1, IdAssigned.name)
      val propertiesReceived1 = recompose(response1, arrayKeysV)

      val response2: Map[Any, List[Any]] = v2Reconstructed.getPropertiesMap
      val idGottenBack2 = extractValue[String](response2, IdAssigned.name)
      val propertiesReceived2 = recompose(response2, arrayKeysV)

      propertiesReceived1.sortBy(x => x.key.name) shouldBe p1.sortBy(x => x.key.name)
      propertiesReceived2.sortBy(x => x.key.name) shouldBe p2.sortBy(x => x.key.name)
      id1.toString shouldBe idGottenBack1
      id2.toString shouldBe idGottenBack2

    }

    scenario("add vertices that follow the format A-B-C") {
      // clean database
      deleteDatabase()

      // prepare
      val id1 = 1
      val id2 = 2
      val id3 = 3
      val now1 = DateTime.now(DateTimeZone.UTC)
      val p1: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now1)))
      val now2 = DateTime.now(DateTimeZone.UTC)
      val p2: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now2)))
      val now3 = DateTime.now(DateTimeZone.UTC)
      val p3: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now3)))

      val pE: List[KeyValue[String]] = List(
        new KeyValue[String](Name, "edge"),
        new KeyValue[String](Number, "38")
      )

      // commit
      new AddVertices().addTwoVertices(id1.toString, p1)(id2.toString, p2)(pE)
      new AddVertices().addTwoVertices(id2.toString, p2)(id3.toString, p3)(pE)

      // analyse
      //    count number of vertices & edges
      val nbVertex = gc.g.V().count().toSet().head
      nbVertex shouldBe 3
      val nbEdges = gc.g.E().count().toSet.head
      nbEdges shouldBe 2

      //    edges
      val edge1: Edge = getEdge(gc, id1.toString, id2.toString, IdAssigned)
      val edge2: Edge = getEdge(gc, id2.toString, id3.toString, IdAssigned)

      val arrayKeysE = Array(Name, Number)

      val edgeReceived1 = recompose(getEdgeProperties(gc, edge1), arrayKeysE)
      val edgeReceived2 = recompose(getEdgeProperties(gc, edge2), arrayKeysE)

      edgeReceived1.sortBy(x => x.key.name) shouldBe pE.sortBy(x => x.key.name)
      edgeReceived2.sortBy(x => x.key.name) shouldBe pE.sortBy(x => x.key.name)

      //    vertices
      val v1Reconstructed = new VertexStructDb(id1.toString, gc.g)
      val v2Reconstructed = new VertexStructDb(id2.toString, gc.g)
      val v3Reconstructed = new VertexStructDb(id3.toString, gc.g)

      val arrayKeysP = Array(IdAssigned, Created)

      val response1 = v1Reconstructed.getPropertiesMap
      val idGottenBack1 = extractValue[String](response1, IdAssigned.name)
      val propertiesReceived1 = recompose(response1, arrayKeysP)

      val response2: Map[Any, List[Any]] = v2Reconstructed.getPropertiesMap
      val idGottenBack2 = extractValue[String](response2, IdAssigned.name)
      val propertiesReceived2 = recompose(response2, arrayKeysP)

      val response3: Map[Any, List[Any]] = v3Reconstructed.getPropertiesMap
      val idGottenBack3 = extractValue[String](response3, IdAssigned.name)
      val propertiesReceived3 = recompose(response3, arrayKeysP)

      propertiesReceived1.sortBy(x => x.key.name) shouldBe p1.sortBy(x => x.key.name)
      propertiesReceived2.sortBy(x => x.key.name) shouldBe p2.sortBy(x => x.key.name)
      propertiesReceived3.sortBy(x => x.key.name) shouldBe p3.sortBy(x => x.key.name)
      id1.toString shouldBe idGottenBack1
      id2.toString shouldBe idGottenBack2
      id3.toString shouldBe idGottenBack3
    }
  }

}
