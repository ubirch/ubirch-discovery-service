package com.ubirch.discovery.models

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.ubirch.discovery.{ Binder, InjectorHelper, TestBase }
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.GremlinConnector
import com.ubirch.discovery.util.RemoteJanusGraph
import org.scalatest.{ Assertion, Ignore }

@Ignore
class StoreSpec extends TestBase {

  /**
    * Simple injector that replaces the kafka bootstrap server and topics to the given ones
    */
  def FakeSimpleInjector(bootstrapServers: String, port: Int = 8183): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, port))
  })) {}

  /**
    * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String, port: Int): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      "core.connector.port",
      ConfigValueFactory.fromAnyRef(port)
    ).withValue(
        "kafkaApi.kafkaConsumer.bootstrapServers",
        ConfigValueFactory.fromAnyRef(bootstrapServers)
      ).withValue(
          "kafkaApi.kafkaProducer.bootstrapServers",
          ConfigValueFactory.fromAnyRef(bootstrapServers)
        )
  }

  RemoteJanusGraph.startJanusGraphServer()

  val Injector = FakeSimpleInjector("")

  implicit val gc: GremlinConnector = Injector.get[GremlinConnector]

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

  //ignore("addVerticesPresentMultipleTimes") {

  scenario("no duplicate -> empty graph") {

    val v1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
    val v2 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "abcde"))
    val edge = generateEdge
    val r = Relation(v1, v2, edge)
    //Injector.get[Storer].
    //Store.addVerticesPresentMultipleTimes(List(r))
    expectedGraphVertexCount(0)
  }

  scenario("duplicate in relation -> 1 vertex") {
    val v1 = VertexCore(Nil, listLabelsVertex.head).addProperty(generateElementProperty("hash", "12345"))
    val v2 = v1
    val edge = generateEdge
    val r = Relation(v1, v2, edge)
    //Store.addVerticesPresentMultipleTimes(List(r))
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
    //Store.addVerticesPresentMultipleTimes(relations)
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
    //Store.addVerticesPresentMultipleTimes(relations)
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
    //Store.addVerticesPresentMultipleTimes(relations)
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
    //Store.addVerticesPresentMultipleTimes(relations)
    expectedGraphVertexCount(1)
  }

  //}
}
