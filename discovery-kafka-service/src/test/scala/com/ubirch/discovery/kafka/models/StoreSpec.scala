package com.ubirch.discovery.kafka.models

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.{ Relation, VertexCore }
import com.ubirch.discovery.kafka.TestBase
import org.scalatest.Assertion

class StoreSpec extends TestBase {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanUpJanus()
  }

  def cleanUpJanus() = {
    gc.g.V().drop().iterate()
  }

  def expectedGraphVertexCount(numberShouldBe: Int): Assertion = {
    gc.g.V().count().l().head.toInt shouldBe numberShouldBe
  }

  feature("addVerticesPresentMultipleTimes") {

    scenario("no duplicate -> empty graph") {

      val v1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
      val edge = generateEdge
      val r = Relation(v1, v2, edge)
      Store.addVerticesPresentMultipleTimes(List(r))
      expectedGraphVertexCount(0)
    }

    scenario("duplicate in relation -> 1 vertex") {
      val v1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v2 = v1
      val edge = generateEdge
      val r = Relation(v1, v2, edge)
      Store.addVerticesPresentMultipleTimes(List(r))
      expectedGraphVertexCount(1)

    }

    scenario("1 duplicate in two relations -> 1 vertex") {
      val v1_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v1_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
      val edge1 = generateEdge
      val v2_1 = v1_1
      val v2_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "fgh"))
      val edge2 = generateEdge
      val relations = List(Relation(v1_1, v1_2, edge1), Relation(v2_1, v2_2, edge2))
      Store.addVerticesPresentMultipleTimes(relations)
      expectedGraphVertexCount(1)
    }

    scenario("1 duplicate in two relations, exchanged place -> 1 vertex") {
      val v1_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v1_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
      val edge1 = generateEdge
      val v2_1 = v1_1
      val v2_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "fgh"))
      val edge2 = generateEdge
      val relations = List(Relation(v1_2, v1_1, edge1), Relation(v2_1, v2_2, edge2))
      Store.addVerticesPresentMultipleTimes(relations)
      expectedGraphVertexCount(1)
    }

    scenario("2 relations no duplicate -> 0 vertex") {
      val v1_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v1_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
      val edge1 = generateEdge
      val v2_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("signature", "abcde"))
      val v2_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "fgh"))
      val edge2 = generateEdge
      val relations = List(Relation(v1_2, v1_1, edge1), Relation(v2_1, v2_2, edge2))
      Store.addVerticesPresentMultipleTimes(relations)
      expectedGraphVertexCount(0)
    }

    scenario("same vertex in the sense of unique properties") {
      val v1_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345")).addProperty(generateElementProperty("signature", "trux"))
      val v1_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
      val edge1 = generateEdge
      val v2_1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
      val v2_2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "fgh"))
      val edge2 = generateEdge
      val relations = List(Relation(v1_2, v1_1, edge1), Relation(v2_1, v2_2, edge2))
      Store.addVerticesPresentMultipleTimes(relations)
      expectedGraphVertexCount(1)
    }

  }
}
