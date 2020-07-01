package com.ubirch.discovery.models

import java.util.Date

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.ubirch.discovery.{ Binder, InjectorHelper, TestBase }
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.GremlinConnector
import com.ubirch.discovery.util.RemoteJanusGraph

import scala.collection.JavaConverters._

class StorerSpec extends TestBase {

  RemoteJanusGraph.startJanusGraphServer()

  def cleanDb(implicit gc: GremlinConnector): Unit = {
    gc.g.V().drop().iterate()
  }

  /**
    * traversal.toString does not produce coherent string at every execution. It'll sometime print
    * AddPropertyStep({key=[x], value=[y]}) or AddPropertyStep({value=[y], key=[x]}). So we create both strings
    * and test that at least one of them correspond
    */
  feature("produce correct gremlin queries") {
    val Injector = FakeSimpleInjector("")
    scenario("getUpdateOrCreateVertices should work for one vertex") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val signature = giveMeRandomString
      val hash = giveMeRandomString
      val timestamp = "1593532578000"
      val label = "UPP"
      val vertex = VertexCore(Nil, label)
        .addProperty(generateElementProperty("hash", hash))
        .addProperty(generateElementProperty("signature", signature))
        .addProperty(generateElementProperty("timestamp", timestamp))
      val gc = Injector.get[GremlinConnector]
      val res = GremlinTraversalExtension.RichTraversal(gc.g.V()).getUpdateOrCreateVertices(List(vertex))
      val stepLabelValue = res._2.verticeAndStep.head._1.name
      val expectedTraversal1 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature)])], [HasStep([hash.eq($hash)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AggregateStep($stepLabelValue), AddPropertyStep({value=[$timestamp], key=[timestamp]}), AddPropertyStep({value=[$signature], key=[signature]}), AddPropertyStep({value=[$hash], key=[hash]}), SelectOneStep(last,$stepLabelValue)]"""
      val expectedTraversal2 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature)])], [HasStep([hash.eq($hash)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AggregateStep($stepLabelValue), AddPropertyStep({key=[timestamp], value=[$timestamp]}), AddPropertyStep({key=[signature], value=[$signature]}), AddPropertyStep({key=[hash], value=[$hash]}), SelectOneStep(last,$stepLabelValue)]"""
      (res._1.toString() == expectedTraversal1 || res._1.toString() == expectedTraversal2) shouldBe true
    }

    scenario("getUpdateOrCreateVertices should work for two vertex") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val (signature1, signature2) = (giveMeRandomString, giveMeRandomString)
      val (hash1, hash2) = (giveMeRandomString, giveMeRandomString)
      val (timestamp1, timestamp2) = ("1593532579000", "1593532577000")
      val (label1, label2) = ("UPP", "SLAVE_TREE")
      val vertex1 = VertexCore(Nil, label1)
        .addProperty(generateElementProperty("hash", hash1))
        .addProperty(generateElementProperty("signature", signature1))
        .addProperty(generateElementProperty("timestamp", timestamp1))
      val vertex2 = VertexCore(Nil, label2)
        .addProperty(generateElementProperty("hash", hash2))
        .addProperty(generateElementProperty("signature", signature2))
        .addProperty(generateElementProperty("timestamp", timestamp2))
      val gc = Injector.get[GremlinConnector]
      val res = GremlinTraversalExtension.RichTraversal(gc.g.V()).getUpdateOrCreateVertices(List(vertex1, vertex2))
      val (stepLabelValue1, stepLabelValue2) = (res._2.verticeAndStep.head._1.name, res._2.verticeAndStep.tail.head._1.name)
      val expectedTraversal1 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature1)])], [HasStep([hash.eq($hash1)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label1]})]]), AggregateStep($stepLabelValue1), AddPropertyStep({value=[$timestamp1], key=[timestamp]}), AddPropertyStep({value=[$signature1], key=[signature]}), AddPropertyStep({value=[$hash1], key=[hash]}), GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature2)])], [HasStep([hash.eq($hash2)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label2]})]]), AggregateStep($stepLabelValue2), AddPropertyStep({value=[$timestamp2], key=[timestamp]}), AddPropertyStep({value=[$signature2], key=[signature]}), AddPropertyStep({value=[$hash2], key=[hash]}), SelectStep(last,[$stepLabelValue1, $stepLabelValue2])]"""
      val expectedTraversal2 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature1)])], [HasStep([hash.eq($hash1)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label1]})]]), AggregateStep($stepLabelValue1), AddPropertyStep({key=[timestamp], value=[$timestamp1]}), AddPropertyStep({key=[signature], value=[$signature1]}), AddPropertyStep({key=[hash], value=[$hash1]}), GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature2)])], [HasStep([hash.eq($hash2)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label2]})]]), AggregateStep($stepLabelValue2), AddPropertyStep({key=[timestamp], value=[$timestamp2]}), AddPropertyStep({key=[signature], value=[$signature2]}), AddPropertyStep({key=[hash], value=[$hash2]}), SelectStep(last,[$stepLabelValue1, $stepLabelValue2])]"""
      (res._1.toString() == expectedTraversal1 || res._1.toString() == expectedTraversal2) shouldBe true
    }

    scenario("getUpdateOrCreateVertices should work for n vertice") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      var vertices: List[VertexCore] = Nil
      val label = giveMeRandomVertexLabel
      for (i <- 0 to 50) {
        vertices = vertices :+ VertexCore(Nil, label).addProperty(generateElementProperty("hash", i.toString))
      }
      val gc = Injector.get[GremlinConnector]
      val res = GremlinTraversalExtension.RichTraversal(gc.g.V()).getUpdateOrCreateVertices(vertices)
      def generateShouldBeStringForOneVertexOneWay(i: Int) = {
        s"""GraphStep(vertex,[]), OrStep([[HasStep([hash.eq($i)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AggregateStep(${res._2.verticeAndStep.toList.find(v => v._2.properties.head.value.toString == i.toString).get._1.name}), AddPropertyStep({key=[hash], value=[$i]}), """
      }
      def generateShouldBeStringForOneVertexOtherWay(i: Int) = {
        s"""GraphStep(vertex,[]), OrStep([[HasStep([hash.eq($i)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AggregateStep(${res._2.verticeAndStep.toList.find(v => v._2.properties.head.value.toString == i.toString).get._1.name}), AddPropertyStep({value=[$i], key=[hash]}), """
      }
      val expectedString1 = {
        val str = new StringBuilder("[")
        for (i <- 0 to 50) { str.append(generateShouldBeStringForOneVertexOneWay(i)) }
        str.mkString
      }
      val expectedString2 = {
        val str = new StringBuilder("[")
        for (i <- 0 to 50) { str.append(generateShouldBeStringForOneVertexOtherWay(i)) }
        str.mkString
      }
      (res._1.toString().contains(expectedString1) || res._1.toString().contains(expectedString2)) shouldBe true
    }

    scenario("getUpdateOrCreateSingle should produce correct traversal") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val signature = giveMeRandomString
      val hash = giveMeRandomString
      val timestamp = "1593532578000"
      val label = giveMeRandomVertexLabel
      val vertex = VertexCore(Nil, label)
        .addProperty(generateElementProperty("hash", hash))
        .addProperty(generateElementProperty("signature", signature))
        .addProperty(generateElementProperty("timestamp", timestamp))
      val gc = Injector.get[GremlinConnector]
      val res = GremlinTraversalExtension.RichTraversal(gc.g.V()).getUpdateOrCreateSingle(vertex)
      val expectedTraversal1 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature)])], [HasStep([hash.eq($hash)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AddPropertyStep({value=[$timestamp], key=[timestamp]}), AddPropertyStep({value=[$signature], key=[signature]}), AddPropertyStep({value=[$hash], key=[hash]})]"""
      val expectedTraversal2 = s"""[GraphStep(vertex,[]), OrStep([[HasStep([signature.eq($signature)])], [HasStep([hash.eq($hash)])]]), FoldStep, CoalesceStep([[UnfoldStep], [AddVertexStep({label=[$label]})]]), AddPropertyStep({key=[timestamp], value=[$timestamp]}), AddPropertyStep({key=[signature], value=[$signature]}), AddPropertyStep({key=[hash], value=[$hash]})]"""
      (res.toString() == expectedTraversal1 || res.toString() == expectedTraversal2) shouldBe true
    }
  }

  feature("execute gremlin queries") {
    val Injector = FakeSimpleInjector("")
    val jgs = Injector.get[DefaultJanusgraphStorer]
    val gc = Injector.get[GremlinConnector]
    scenario("getUpdateOrCreateSingleConcrete should add one vertex with all its props") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val signature = giveMeRandomString
      val hash = giveMeRandomString
      val timestamp = "1593532578000"
      val label = giveMeRandomVertexLabel
      val vertex = VertexCore(Nil, label)
        .addProperty(generateElementProperty("hash", hash))
        .addProperty(generateElementProperty("signature", signature))
        .addProperty(generateElementProperty("timestamp", timestamp))
      cleanDb(gc)
      jgs.getUpdateOrCreateSingleConcrete(vertex)
      validateVertex(vertex, gc) shouldBe true
    }

    scenario("getUpdateOrCreateSingleConcrete should add two vertices with all there props") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val (signature1, signature2) = (giveMeRandomString, giveMeRandomString)
      val (hash1, hash2) = (giveMeRandomString, giveMeRandomString)
      val (timestamp1, timestamp2) = ("1593532579000", "1593532577000")
      val (label1, label2) = (giveMeRandomVertexLabel, giveMeRandomVertexLabel)
      val vertex1 = VertexCore(Nil, label1)
        .addProperty(generateElementProperty("hash", hash1))
        .addProperty(generateElementProperty("signature", signature1))
        .addProperty(generateElementProperty("timestamp", timestamp1))
      val vertex2 = VertexCore(Nil, label2)
        .addProperty(generateElementProperty("hash", hash2))
        .addProperty(generateElementProperty("signature", signature2))
        .addProperty(generateElementProperty("timestamp", timestamp2))
      cleanDb(gc)
      jgs.getUpdateOrCreateVerticesConcrete(List(vertex1, vertex2))
      validateVertex(vertex1, gc) shouldBe true
      validateVertex(vertex2, gc) shouldBe true
    }

    scenario("getUpdateOrCreateSingleConcrete should work for n vertices") {
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      var vertices: List[VertexCore] = Nil
      val label = giveMeRandomVertexLabel
      for (i <- 0 to 50) {
        vertices = vertices :+ VertexCore(Nil, label).addProperty(generateElementProperty("hash", i.toString))
      }
      jgs.getUpdateOrCreateVerticesConcrete(vertices)
      for (vertex <- vertices) {
        validateVertex(vertex, gc) shouldBe true
      }
    }

    scenario("create edge should work") {
      // prepare: create the vertices
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val (signature1, signature2) = (giveMeRandomString, giveMeRandomString)
      val (hash1, hash2) = (giveMeRandomString, giveMeRandomString)
      val (timestamp1, timestamp2) = ("1593532579000", "1593532577000")
      val (label1, label2) = (giveMeRandomVertexLabel, giveMeRandomVertexLabel)
      val vertex1 = VertexCore(Nil, label1)
        .addProperty(generateElementProperty("hash", hash1))
        .addProperty(generateElementProperty("signature", signature1))
        .addProperty(generateElementProperty("timestamp", timestamp1))
      val vertex2 = VertexCore(Nil, label2)
        .addProperty(generateElementProperty("hash", hash2))
        .addProperty(generateElementProperty("signature", signature2))
        .addProperty(generateElementProperty("timestamp", timestamp2))
      cleanDb(gc)
      val res = jgs.getUpdateOrCreateVerticesConcrete(List(vertex1, vertex2))
      val timestamp3 = "1593542577000"
      val edge = EdgeCore(Nil, "UPP->DEVICE").addProperty(generateElementProperty("timestamp", timestamp3))
      val dumbRelation = DumbRelation(res.toList.head._2, res.toList.tail.head._2, edge)
      // create
      jgs.createRelation(dumbRelation)

    }
  }

  def validateVertex(vertexCore: VertexCore, gc: GremlinConnector)(implicit propSet: Set[Property]): Boolean = {
    val maybeVertex = vertexCore.properties.find(p => p.isUnique) match {
      case Some(uniqueProp) => gc.g.V().has(uniqueProp.toKeyValue).l().headOption
      case None => gc.g.V().has(vertexCore.properties.head.toKeyValue).l().headOption
    }
    maybeVertex match {
      case Some(vertex) =>
        var res = true
        for (prop <- vertexCore.properties) {
          // special case for timestamp (as always..)
          val propOnGraphValue = gc.g.V(vertex).value(prop.keyValue.key).l().head
          if (prop.keyName == "timestamp") {
            val actualProp = propOnGraphValue.asInstanceOf[Date]
            val shouldBeDate = new Date(prop.value.toString.toLong)
            if (actualProp != shouldBeDate) res = false else {}
          } else {
            if (propOnGraphValue != prop.value.toString) res = false
          }
        }
        if (gc.g.V(vertex).hasLabel(vertexCore.label).l().isEmpty) {
          res = false
        }
        res
      case None => false
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
