package com.ubirch.discovery

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.ubirch.discovery.models.{ EdgeCore, KafkaElements, Relation, VertexCore }
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.consumer.{ AbstractDiscoveryService, DiscoveryServiceSendErrorOk }
import io.prometheus.client.CollectorRegistry
import org.scalatest.PrivateMethodTester

class DiscoveryServiceUnitTests extends TestBase with PrivateMethodTester {

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
  }

  feature("parseRelations") {
    val Injector = FakeSimpleInjector("")
    def FakeInjectorSendOK(bootstrapServers: String, port: Int = 8183): InjectorHelper = new InjectorHelper(List(new Binder {
      override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, port))
      override def DiscoveryService: ScopedBindingBuilder = bind(classOf[AbstractDiscoveryService]).to(classOf[DiscoveryServiceSendErrorOk])
    })) {}
    scenario("parse relation successfully given a correct relation") {
      val ds = Injector.get[AbstractDiscoveryService]
      val rawRelation = "[ { \"v_from\": { \"label\": \"PUBLIC_CHAIN\", \"properties\": { \"hash\": \"hashPubChain\", \"public_chain\": \"IOTA_TESTNET_IOTA_TESTNET_NETWORK\", \"timestamp\": 1593173111922 } }, \"v_to\": { \"label\": \"MASTER_TREE\", \"properties\": { \"hash\": \"hashMasterTree\" } }, \"edge\": { \"label\": \"PUBLIC_CHAIN->MASTER_TREE\", \"properties\": { \"timestamp\": 1593173111922 } } } ]"
      val parsedRelations = ds.parseRelations(rawRelation)
      parsedRelations.size shouldBe 1
      val relation = parsedRelations.get.head
      relation.edge shouldBe EdgeCore(Nil, "PUBLIC_CHAIN->MASTER_TREE").addProperty(generateElementProperty("timestamp", "1593173111922"))
      relation.vFrom shouldBe VertexCore(Nil, "PUBLIC_CHAIN")
        .addProperty(generateElementProperty("timestamp", "1593173111922"))
        .addProperty(generateElementProperty("public_chain", "IOTA_TESTNET_IOTA_TESTNET_NETWORK"))
        .addProperty(generateElementProperty("hash", "hashPubChain"))
      relation.vTo shouldBe VertexCore(Nil, "MASTER_TREE")
        .addProperty(generateElementProperty("hash", "hashMasterTree"))
    }

    scenario("parse incorrect relation should throw an error") {
      val ds = Injector.get[DiscoveryServiceSendErrorOk]
      val rawRelation = "[ { \"\": { \"label\": \"PUBLIC_CHAIN\", \"properties\": { \"hash\": \"hashPubChain\", \"public_chain\": \"IOTA_TESTNET_IOTA_TESTNET_NETWORK\", \"timestamp\": 1593173111922 } }, \"v_to\": { \"label\": \"MASTER_TREE\", \"properties\": { \"hash\": \"hashMasterTree\" } }, \"edge\": { \"label\": \"PUBLIC_CHAIN->MASTER_TREE\", \"properties\": { \"timestamp\": 1593173111922 } } } ]"
      ds.parseRelations(rawRelation) shouldBe None
      ds.counter shouldBe 1
    }

    scenario("facing an empty message, an error should be sent") {
      val ds = Injector.get[DiscoveryServiceSendErrorOk]
      val rawRelation = ""
      ds.parseRelations(rawRelation) shouldBe None
      ds.counter shouldBe 1
    }
  }

  feature("get distinct vertices") {
    val Injector = FakeSimpleInjector("")
    implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
    scenario("should return 3 vertices in this relation of 4 with 3 distinct") {
      val relation1 = generateRelation
      val vertice2_1 = generateVertex
      val vertice2_2 = relation1.vTo
      val edge2 = generateEdge
      val relation2 = Relation(vertice2_1, vertice2_2, edge2)
      val getDistinctVertices = PrivateMethod[List[VertexCore]]('getDistinctVertices)
      val res = AbstractDiscoveryService invokePrivate getDistinctVertices(List(relation1, relation2), propSet)
      res.sortBy(_.label) shouldBe List(relation1.vFrom, relation1.vTo, vertice2_1).sortBy(_.label)
    }

    scenario("should return 2 vertices in a relation of 2 distinct vertices") {
      val relation = generateRelation
      val getDistinctVertices = PrivateMethod[List[VertexCore]]('getDistinctVertices)
      val res = AbstractDiscoveryService invokePrivate getDistinctVertices(List(relation), propSet)
      res.sortBy(_.label) shouldBe List(relation.vFrom, relation.vTo).sortBy(_.label)
    }

    scenario("should return all of them") {
      var relations = List(generateRelation)
      for (_ <- 0 to 50) {
        relations = relations :+ generateRelation
      }
      val getDistinctVertices = PrivateMethod[List[VertexCore]]('getDistinctVertices)
      val res = AbstractDiscoveryService invokePrivate getDistinctVertices(relations, propSet)
      val allVertices = relations.flatMap(r => List(r.vFrom, r.vTo))
      res.sortBy(_.label) shouldBe allVertices.sortBy(_.label)

    }

    scenario("should return 1 with two vertices different but equal in term of unique props, should return merged vertex") {
      val label = giveMeRandomVertexLabel
      val hash = generateElementProperty("hash")
      val t1 = generateElementProperty("timestamp", giveMeATimestamp)
      val t2 = generateElementProperty("timestamp2", giveMeATimestamp)
      val signature = generateElementProperty("signature")
      val v1 = VertexCore(Nil, label)
        .addProperty(hash)
        .addProperty(signature)
        .addProperty(t1)
      val v2 = VertexCore(Nil, label)
        .addProperty(hash)
        .addProperty(t2)
      val mergedVertex = VertexCore(Nil, label)
        .addProperty(t2)
        .addProperty(hash)
        .addProperty(signature)
        .addProperty(t1)
      // here v1 and v2 are unique in term of iterable properties
      val edge = generateEdge
      val r = Relation(v1, v2, edge)
      val getDistinctVertices = PrivateMethod[List[VertexCore]]('getDistinctVertices)
      val res = AbstractDiscoveryService invokePrivate getDistinctVertices(List(r), propSet)
      res.size shouldBe 1
      res.head shouldBe mergedVertex

      // testing in the other way
      val r2 = Relation(v2, v1, edge)
      val res2 = AbstractDiscoveryService invokePrivate getDistinctVertices(List(r2), propSet)
      res2.size shouldBe 1
      res.head shouldBe mergedVertex
    }
  }

  feature("Validate vertices are correct") {
    scenario("should return None given valid data") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      var relations = List(generateRelation)
      for (_ <- 0 to 50) {
        relations = relations :+ generateRelation
      }
      val vertices = relations.flatMap { r => List(r.vFrom, r.vTo) }
      val validateVerticesAreCorrect = PrivateMethod[Option[List[VertexCore]]]('validateVerticesAreCorrect)
      val res = AbstractDiscoveryService invokePrivate validateVerticesAreCorrect(vertices, propSet)
      res shouldBe None
    }

    scenario("should return Some(v1) given v1 without iterable property and v2 with iterable property") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val vertexWithoutIterableProperty = VertexCore(Nil, "UPP").addProperty(generateElementProperty("coucou", "salut"))
      val vertexWithIterableProperty = VertexCore(Nil, "UPP").addProperty(generateElementProperty("hash", "hey"))
      val validateVerticesAreCorrect = PrivateMethod[Option[List[VertexCore]]]('validateVerticesAreCorrect)
      val res = AbstractDiscoveryService invokePrivate validateVerticesAreCorrect(List(vertexWithoutIterableProperty, vertexWithIterableProperty), propSet)
      res shouldBe Some(List(vertexWithoutIterableProperty))
    }
  }

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

}
