package com.ubirch.discovery.models

import java.util.Date

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.ubirch.discovery.{ Binder, InjectorHelper, TestBase }
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.process.Executor
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.connector.GremlinConnector
import com.ubirch.discovery.services.consumer.AbstractDiscoveryService
import com.ubirch.discovery.util.RemoteJanusGraph
import gremlin.scala.{ Key, KeyValue, Vertex }
import io.prometheus.client.CollectorRegistry
import redis.embedded.RedisServer

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success }

class StorerSpec extends TestBase {

  //RemoteJanusGraph.startJanusGraphServer()

  def cleanDb(implicit gc: GremlinConnector): Unit = {
    gc.g.V().drop().iterate()
  }

  override def beforeAll() = {
    val redis = new RedisServer(6379)
    redis.start()
    Thread.sleep(8000)
  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
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

  ignore("execute gremlin queries") {
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

    scenario("lookThenCreate") {
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
      jgs.lookThenCreate(vertex)
      validateVertex(vertex, gc) shouldBe true
    }
  }

  ignore("concurency") {
    scenario("test creation vertices") {
      val Injector = FakeSimpleInjector("")
      val jgs = Injector.get[DefaultJanusgraphStorer]
      val gc = Injector.get[GremlinConnector]
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      implicit val ec: ExecutionContext = Injector.get[ExecutionContext]
      var vertices: List[VertexCore] = Nil
      val label = giveMeRandomVertexLabel
      for (i <- 0 to 50) {
        vertices = vertices :+ VertexCore(Nil, label).addProperty(generateElementProperty("hash", i.toString))
      }
      val lVertices = Seq(vertices, vertices, vertices, vertices, vertices, vertices, vertices)
      val executor = new Executor[List[VertexCore], Map[VertexCore, Vertex]](lVertices, jgs.getUpdateOrCreateVerticesConcrete(_), 8)
      executor.startProcessing()
      executor.latch.await()
      for (vertex <- vertices) {
        validateVertex(vertex, gc) shouldBe true
      }
    }

    scenario("create edge should work") {
      val Injector = FakeSimpleInjector("")
      val jgs = Injector.get[DefaultJanusgraphStorer]
      val gc = Injector.get[GremlinConnector]
      // MT connected to 10 ST, each ST connected to 50 ST
      val ds = Injector.get[AbstractDiscoveryService]
      implicit val ec: ExecutionContext = Injector.get[ExecutionContext]
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      // create MT
      val hashMT = generateElementProperty("hash")
      val timestampMT = generateElementProperty("timestamp", giveMeATimestamp)
      val master_tree = VertexCore(Nil, "MASTER_TREE")
        .addProperty(hashMT)
        .addProperty(timestampMT)
      // create 10 ST linked to 50 ST each
      var mapSlaveTrees = scala.collection.mutable.Map[VertexCore, List[VertexCore]]()
      for (_ <- 1 to 100) {
        val hashSTRoot = generateElementProperty("hash")
        val timestampSTRoot = generateElementProperty("timestamp", giveMeATimestamp)
        val slave_tree_root = VertexCore(Nil, "SLAVE_TREE")
          .addProperty(hashSTRoot)
          .addProperty(timestampSTRoot)
        var lSTlow = List[VertexCore]()
        for (_ <- 1 to 50) {
          val hashSTLeaves = generateElementProperty("hash")
          val timestampSTLeaves = generateElementProperty("timestamp", giveMeATimestamp)
          val slave_tree_leave = VertexCore(Nil, "SLAVE_TREE")
            .addProperty(hashSTLeaves)
            .addProperty(timestampSTLeaves)
          lSTlow = lSTlow :+ slave_tree_leave
        }
        mapSlaveTrees += (slave_tree_root -> lSTlow)
      }

      val listST_ST = for {
        l <- mapSlaveTrees.toList
      } yield {
        val lRoot = l._1
        for {
          lLeave <- l._2
        } yield {
          Relation(lRoot, lLeave, EdgeCore(Nil, "SLAVE_TREE->SLAVE_TREE")
            .addProperty(generateElementProperty("timestamp", giveMeATimestamp)))
        }
      }
      logger.info("st_st size: " + listST_ST.flatten.size)
      val MT_ST_relations = mapSlaveTrees.keys.toList.map(v => Relation(master_tree, v, EdgeCore(Nil, "MASTER_TREE->SLAVE_TREE")
        .addProperty(generateElementProperty("timestamp", giveMeATimestamp))))
      logger.info("mt_st size: " + MT_ST_relations.size)
      val allRelations = listST_ST.flatten ++ MT_ST_relations
      logger.info("all r size: " + allRelations.size)

      Future(ds.store(allRelations)).onComplete(_ => println("finished 1"))
      Future(ds.store(allRelations)).onComplete(_ => println("finished 2"))

      val resDS = ds.store(allRelations)
      logger.info("finished storing")
      resDS match {
        case Some(value) => value foreach { r =>
          r._2 match {
            case Failure(exception) =>
              logger.error("error", exception)
              fail()
            case Success(_) =>
          }
        }
        case None => fail()
      }
      for (r <- allRelations) {
        if (!validateRelation(r, gc)) {
          logger.error(s"Failed for relation ${r.toString}")
          fail()
        }
      }
    }

    scenario("concrete case") {
      val Injector = FakeSimpleInjector("")
      val gc = Injector.get[GremlinConnector]
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val rel = """[ { "v_from": { "label": "UPP", "properties" : { "hash": "nq7dqEoLtv/BVUsXzfFTqwzgTAfD8ZzTvO0ksrIpogSCvevWkEolk7AYn+XjcTXvibRDvyv6tY2TdhBs+UxdHA==" ,"signature": "MYnFm/oNeVI0AYjfzutyzG+J3UI1rY1jYGR9P6b+GQOmRpany3IS7/i2gAo6vo2ryfPeAZrOi6OpnpnJJxKNCw==", "timestamp": "1594378982990" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "b740422a-fb00-41ad-b8fa-d113e9734821" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378982990" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "vlMslnpsrh7UWsCtlpN/KBuYi0KiR+mkT9CKzX+CknmPaZi5jD1NC84HakZ5UmCRen0B0pge5MkJMkhdE1xeDA==" , "signature": "QAAyAhqzC3P3F0cif0qOcmc3knvJ2FkjxbiqlxI9/0sbaPOvG8SFk9dOyZ13q4EWnKiJfENKnti5qBQEY26bDw==" ,                    "timestamp": "1594378983327" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "62899d93-badb-4d3a-a11e-e969779219fc" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983327" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "WD5sBMH1r8Gp5rQG0YE+5+p3gcQCs0c8nDDId2cDQhyv6kHL9fX9eXYoT/iLT4ezS3zgPt0oCcBBLGeCFyvasg==" ,                    "signature": "rTpIkSotvK6ams3zkkFaMiruPg0XNGyW9rp0ktTPb9WW6etTcTOwUwzYMRfgNwe0YQirFSH/kf67IGWBCDybBA==" ,                    "timestamp": "1594378983142" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "eed6ef70-68c1-4479-9e77-359af39e11ca" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983142" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "QjYrA+GBmlnHOKPj26zB7Zhq5PxfPXnZJX5jczZFBiFahQzGyqAYYhkAr9TxaVseSexHS07O5lS5aIuWWXB31Q==" ,                    "signature": "569N2oDrXc1NsDVzEqs1b6uTDWU7noAC5+ikEBEUi5n6mYHKFuq/7wzCJj5yASjnLIG2bHL/B67u5Etdk/yXCA==" ,                    "timestamp": "1594378983408" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "ebad3a49-2dd6-457a-a536-11bf2acc85c0" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983408" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "iz0KbPrTFdsvLSOQk3tLhl3TLuj25x7ZwlwJWGjWG89cWsqQzO0+vEpKio8k01K1HVv5cVa4MET9ZF2BoOm3zA==" ,                    "signature": "uBaJyJ/rjgsFCDE8qPLC0PDZ9/VCtRmrJAUgDc33rlKZS4kYlS2++wEcTbkufEiDIRbajFYLIrkWEKSuvQq+Dg==" ,                    "timestamp": "1594378983290" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "80ab3e01-6983-46e5-82b7-874894cba958" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983290" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "upyq87pjOf/QAuMZ2ss8U0ZSKb09wEoOWgd/meEhOGwvqliZVUAr7jtZbn4CheiOJHQQlEpXwB3Yw7Io8Wyyyw==" ,                    "signature": "PdBjTrOIuYhXU4mcDoX2LqGNsH+sTIcezg+/pOb4usa1At/9dJoSONtQcJvA+NinGTOK3pnaMhLdLILLBOBZCw==" ,                    "timestamp": "1594378983090" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "2727d9c2-e233-401c-8ce9-8a42e093275c" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983090" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "XBtvG8nC97Sb6Rpug2PbhvqPSQMN3QOJ9wQsFuZI7FK1SSbFcDETZv0Jmgd4xsYjvkFa9mTVIDkr4uZI/n7QlQ==" ,                    "signature": "tHahuIHy0xoW06fZDZ1vCTScTPSsGs9iBK+EnHXALg4IDjzc94ROitQ7ZkZlQLdARC7PMnCznZfccyEA1wbpAg==" ,                    "timestamp": "1594378983012" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "9c7b8233-9e62-4245-8053-ec2206b41de2" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983012" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "uzoBoGI7ZSbTtbf7wVH8fzfTNco4oaXC7w1AZlt9z1q/ptzVEnq3+SOSdS35CSuOWzs4Y9uLK1m/5ffVqiSgag==" ,                    "signature": "odQ2f9uPnxu0dTKnEe6m8Hth20p2OHA4ZnKhhwmvE1mgnbEf7Jwr1WxjJs1GKa3TW2BKZ3D0DqgvjjDwGeuqAA==" ,                    "timestamp": "1594378982695" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "815b917f-8338-423a-ada6-92dc5c4032f6" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378982695" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "DtOmD51tIshufsxv2weEZ4c7pYLcjAlQkird+IQLGjZ06ZHo1rKxWWNA+NM2V9HRqQZWy2+/5GqwWI2pgQhNbQ==" ,                    "signature": "UnufJJvwTAT4P7WUdz1E1v+TiNnR6iCCo/rfH3HbKGC33UXsHuKB8gOhXk7xoivQqojWNyJl0z89rQldLRekBg==" ,                    "timestamp": "1594378982699" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "e9205927-6407-42bb-a8bf-31acb5cae71e" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378982699" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "FB3pFOT511oJo0tmmQszoIqMFWgKjLdj+m162JplyDbdA3CGgV23J/dmCoSJCQ8ATjIu33x2FlviQ99ltgOc+g==" ,                    "signature": "87sH/tttXeD4Dpe9J7bmBz5RKW9TDy/OwkULz2WxtfBQ/TNIG1XOIqatD97R7qhntyaYLQUxUARYh5THo9W2AA==" ,                    "timestamp": "1594378983203" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "c9e6a566-508e-478a-ae22-87be9a2e550e" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983203" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "t/1ht39JCYVB6GaK11nCEvt0TjJFjnRtpNv88hTs3QW6H4IoNAKJiXZo4JoHRbfLKtidhADHG1uEqEcRGN7oTw==" ,                    "signature": "4jLa359p/BHyX4+aAt9PHurhM3bJTg4nhpBfDM2bo2UreSCJkx6SqEklDjb2xWLyXBbjNc3SV6p5bpEBjVq+CQ==" ,                    "timestamp": "1594378983167" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "0ca449f0-cbea-4073-9b92-9fcca1605bba" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983167" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "YPyb08RxU3zY2hhSe+8rK/oDuYAlOQ7VSZd0OvhmGSgLkYzuVJhWMMFvFv+hU/YYY4Utk8DmcBZUe+/1F1HVYw==" ,                    "signature": "X7O9KT62+d1quafgpK7WZTtkNwQw5iJeKnxFkq5/u4BrfSTpqm8Wr6+S0PZc30Ow27gihP6r66YRzucg5OkGCQ==" ,                    "timestamp": "1594378983589" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "19acccb5-ceb2-4087-a133-b6b2fba874a3" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983589" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "L3joPIqSVSH129a8vPfsvTPIBgju5553KDUcFmKqqLRK95YFweZll9S8sYFs18J1+fzZoI+kX8Jcp147jr1jaw==" ,                    "signature": "Kso4I13p1SuU0IapzahBYKkOGCSclkUvmDExsdyvgGtd1G2qRx82d6GdSvBZKIOtrkksrjf/S64Q9ucnNytDCg==" ,                    "timestamp": "1594378983206" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "d8364a14-92db-456f-a4fe-f30a24ddfb31" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983206" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "QqlYlDiMxJ2D/VIvK+tPeTyNYydun1Zf3lelxuk6gT7uZspfMqMn2+vwaPdj83IxTTtToLsio+jt76/kD61IvQ==", "signature": "oQ4GB4RX+CQpq5KvxAAU+Z5bpcxNR6UTIUdtg7hx1MRubDCGD8Nwv+Rs2U/HFFxyCLYmWUFgp4S4twf6W5B0Cw==" ,                    "timestamp": "1594378983322" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "7f2cbd2d-b51d-458a-8db0-1bfd709fdac8" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983322" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "tHB7UMgtvnFfIwoweGDDzXUFwqQ3BuJFYzFKADRozCnqzz4Vx+jc1i3ZDWBFGx+7NMQ8suWbGOUa+QOo+vwFPw==" ,                    "signature": "R0ilbu62Aw3+pKIfB9No6Za0zq8V211vnbEQcPkLiUQzKBEaRRBG4QyYNOAiIhaYiBYV2Wnlvm1QNel924RmDg==" ,                    "timestamp": "1594378983437" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "ca2c4d6c-0cb9-486e-aed6-a429d1b73d03" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983437" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "sT8tn6el+0BdZgtAAOjdEjQyFsHvsuE7byp7nLjRAHbLpSI+V4h0yn2KNZ78xi8uKh7/1m9JXGWSI8AUmhA5CA==" ,                    "signature": "enkrddxMBtiLMj1gyUj9KpAdUj7nhVdZFgQuin5EZC4G9CT/SBOvuKixAem397jbKwB/rDSxb5vNGvP6rPXwCw==" ,                    "timestamp": "1594378983681" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "257c4e6d-23ca-405c-b51a-3045e02d8d72" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983681" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "+vvvlVD47nix+f6Kgey6Uq/JpYJq1GGIJY5kjHPiJo0oYhvzQa8h2h3bXs+1PPSUuEPk4MEka7bS8/lkP2ee6g==" ,                    "signature": "o6ESqWjqIWtjzUJo+h/x+Ftr/esD57FizZzcN73AlnXmHJGBL4IcTXZmdGoGpw9jn6U+pfJVoKZISvGgT2dBCg==" ,                    "timestamp": "1594378983560" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "f12984f8-5549-4891-bb5e-3b9542a3d878" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983560" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "jVIzYRMDYqlXgmNEk35HjOYJRl1bpuZUghkaFBDl9NivpaHQ6p03e35/QH42R2CYaa1DDZkcq71KPx+1sfyk9g==" ,                    "signature": "Dno/KLGAYU9HGIph0SbbA7AA2DAXeuc8VF0l3AKGFjEMx+iJdrK1ZSPrpSG6+hhwrY6WIOkkuVgeXQtMxsUuBg==" ,                    "timestamp": "1594378983614" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "fb326ec6-b40f-491e-93fa-481f3854b201" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983614" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "sFgWGPCl1KtU387Bagt8K4HIEgeZMXuymisAwzfAcYh3huCe5pbG2pGXppyyGN2jJeAt5XR4wLVvWHXN6gWgUA==" ,                    "signature": "ffeqGOddZv+nTIThwMMQLDCvhc87oJDOQTlNjm1jiFPwc5r9AsyAMfLWheNSANXTXOANZoBtzUgpwv95v9+2BA==" ,                    "timestamp": "1594378983437" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "479dba00-b019-4d0e-8fe3-0d2ff52b5de1" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983437" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "dGnDIz3HIXNpwuV7tm+pw4a1mlAYIHpGc1xiUdnHE7ERY2fNN06fGQmgKhLp5a9XO6jmqatd0cdRW1uma+il6Q==" ,                    "signature": "VP+3UEAEjdvYEHIEgeUZki3zeBzUF7JuQ2GjYPypC0Eyx6wrorQEF4MWwKMwRohy7OH5pJCaCClL25Zr2F29BQ==" ,                    "timestamp": "1594378983773" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "0c6adf34-d6ad-461b-8d5d-81c7c0439bc0" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983773" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "Byv5LT+NzRp2ksikgT100B9ZXLH3aYkjNWTFA4snnjsMIpXxaWmtxAIUcXy36Jj7pBqnz8cmJTm925vMD0V/lg==" ,                    "signature": "1b49lD/njtMT2RKEHjQPuKDP0ia18noZ1kGGohBlT8lPiJCiIe4cR5chMJWtL7mfneRvHeTAvYvr4CH6MnKQCg==" ,                    "timestamp": "1594378983472" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "8fb09df9-547c-4e7a-8d40-780f7148798b" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983472" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "XuU5GPkHtrvh2t1qnDAjDk7niN7WHE/FlWDytSn2syBRVCBKYie+mcRzrFQg9ae9oXG9HRagjPUvJ2vqDX6bBw==" ,                    "signature": "cJHQzWD2FMfQM7ONUJAF0SmL+XhOzP64uu2m960Y3iy/IA2U0zt2m30Z9szkMKSbX97Bvnyrxcsgoig4+zxtBQ==" ,                    "timestamp": "1594378983249" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "1db45f06-d514-436a-85a7-b0d36c67ac80" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983249" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "gI2T1+TRRepm0MLKq0DlxbNYEUETCP0Qx9uk18iq5LY+iHNvWIxjkUZhFvufAIPfBQ9d0UiuahzF2n/tglHmiQ==" ,                    "signature": "/1lTCihfzUlFV4ahF8VAjjLO+W5pN+ibdRwhmG8HlORvTzezvKTXBCGdhKHl28umtJ6FOCoMwAYrGvPnbP8lAw==" ,                    "timestamp": "1594378983664" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "8070c67c-a686-48dc-bebe-93d07c2b875d" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983664" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "OG4TC3grVMhsppKHdTB/kNpUJNxSG1JLkh2V01UxKKDOe2iZh7G8D5uGXFux2atkPdAUOvH1vh1qW8HhQAxtLA==" ,                    "signature": "HftdRPYglwjvaCPr66MD3/v2M00hhTf2+lcdCrO68nTz0eCBhV9CHe2WwnlO6WhDDCRg1QUeK7rpYooDfMiHDg==" ,                    "timestamp": "1594378983654" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "32e4e02d-4ae6-4138-92a4-96a6f4f959e1" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983654" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "3g3OvHg+Hl4mlXJPwZWhRPzcX0L3AbWO33GMokIqjh6Wico8hrvq+ngkb7AjebNLF934RWvrPSUJTMNBfOV8Zw==" ,                    "signature": "O9AJfqgLuWLJdUFohp3ZW4ZwBk7IN6rhYjvbI2fK+61qlJkhvT6NoXOYXTFKyHjntIKPMlv7oGApyFHv+rO8DA==" ,                    "timestamp": "1594378983549" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "479df6f1-efd0-41a5-bb88-5195fb03384d" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983549" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "4fFZzmmBBdO8S7klqlhTHjPSnnJeyFhfHSPLHaaL5l7CNVYVA3QpCTu2qCKuz3iae3+O/i556/FIL5BOkmvcmA==" ,                    "signature": "AuDgiamVamBducDylC+P+vswD7PMHLc8R6MLIODpepZGln1RrAFVWcC7sM6mrR+OHiY4dJxztiPyuuPkEVxlBg==" ,                    "timestamp": "1594378983499" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "f1018c84-225b-4dac-86bc-505861b11876" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983499" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "qxq2YZqeaWFupHmQwWdBF+vLx4vTcZKzz8AWlDVxIj8xm+onqQBxPEFwG18IZKwYF9m4pt2w3xJ20TQh0yxhLw==" ,                    "signature": "qaauEAUnDXWUi9wZX1WBLXy7fSzITyDTVcWy4C5L5zSsVTmLrbn4psIxdd+xTrgtFCUPc9SL3kMMOLa8PefQCA==" ,                    "timestamp": "1594378983806" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "5e213bf7-62b7-441b-b79a-409961a5daaf" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983806" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "bYJfy+Az0VL1JQKdfvDsybxWWpzOWFXDwKJ+uQCDW6bE1OAVqz2lnV8ZBIqdY9vjUjidpvPUVKa+0dEE468jzg==" ,                    "signature": "W/C2T1MAeLtqO1dkHiRjp4CuiW9sbBNAB9oreoV6tOL1xuAz3qArzZbv+71+fVeRjGV4PIeSf+xnNRC2gx+eBg==" ,                    "timestamp": "1594378983684" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "cf41710a-f09c-43b9-b7c1-beea31bc31f7" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983684" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "inF+mXel/fxX4AUsXHSqPVrZi2fqiS+llPauixsKFrC4s3y24X3VBsbssr7IxMlzipuBddpR9NqXfMrE7qHgRQ==" ,                    "signature": "Fhp9ZjLh2vLqlyRuidserACityKW1dcuHgSp4pqQ2nBecmAoLWjmU+LLP3a3XUNDgySPRZbTY8XF4SHZ8hu3Dg==" ,                    "timestamp": "1594378983793" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "5e019ca9-2db2-4d40-a790-b4ae9f8e1a68" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983793" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "kN3H0lszIG+zXVyqRhtWFuPPm0gobgkrfy8nwcL/OOmCw+sRYgHzrviTKEgt5oQA6tqGN9iUHIhoezerXlc8YQ==" ,                    "signature": "qqSqnynAsFvBelVgcAUpZQIDS4aPW8I9dBpbDjr7wvVz/5gZsqD1gFOY8ZF5jvxFDeLSI5eSNOgd2W7m/c5jCg==" ,                    "timestamp": "1594378984095" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "dd2448b1-e5e9-40c9-81c5-b2e121ff7f42" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984095" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "ElIPBZBY1HCnJNCos6k4L6RMyjbirgFWWN2BXEdx6OPtPuEfc+mHc3rDuy7v+5Eiey/KODOZYM3Xy6a0aap+qg==" ,                    "signature": "3QaDUt4g3+YMsUnFTwZbBWw/S2kMpNqADrlKNgRVOw02+s6aDT1sWhvwTFqYFUWcraIixHQHfJx4cUVIXgsWDQ==" ,                    "timestamp": "1594378983846" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "175c5c08-2de9-45d5-b20a-7bb893218dc1" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983846" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "pY3hQp7B6O23hJ4M/Av+RJEQc7l7cJe5pMWTXw0RHIZ4C2xxumuqjk1+YfRdhEGrU+AZT5x+PEG4NsAJP2VAig==" ,                    "signature": "NEd76bRHy0CMv2TP9WI60nFWNMaYx1uRKzY5tvuNyJyqHIi04yO7WEKE19dtLnvkqApo8L6rDIhQ/BTshW+RDQ==" ,                    "timestamp": "1594378983767" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "11d08370-3c19-444f-b9aa-167e115d9d81" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983767" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "ZfchtD0Lf8cjMSE+p2to/KPm2D4owWZTbGCrw8Nl5QsXP28rSzSed1d7Ig7kNBzbLcKZaFuJ6H1dyhSn3rQ1zA==" ,                    "signature": "jo6q5K+io2z+yqAU43Zxl1KEJYkikpJAJ9W/fUI1pwby4f+vnjMGgotDCY1vDjZJqMVIH7NWg4fBV1FjVs4tCQ==" ,                    "timestamp": "1594378983498" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "4c7e228e-274f-491e-b0ca-9bd0ba1055a3" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983498" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "8WutEn57INEUBN4jC2v3tDAGJqEC2+k5V6fLVcTyd30oYNNm6GZ9Zd9B2afTQGvR1yLkhfKeEvXivkJDkK6yFw==" ,                    "signature": "zDaHybpcRjH7WTYkGIRYV1cLi/UkvykC5XB2Jovc8Lbpn0TUSTW5+sQCw931ym2xl9f5UFp7J4/4105xncBeAA==" ,                    "timestamp": "1594378983690" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "f45dc31a-803b-4cd1-acc3-06d03c4bfb8f" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983690" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "ApLpbRcQ6xawd10XxEKedkgCviXlBm4sprHpsPVjl/l5CytQwfVFFgU94qR6t68gO8I5Q0LK0s9y/VDO1BNtMQ==" ,                    "signature": "AQUUp4Pr30Xeoe0KTeVE04eqT5d+ZQcbOiH9hHsGvn/KUNd5jaBsCUFQX77ZLYUGy0Z7nCmtWD31Hv8TQF5NBQ==" ,                    "timestamp": "1594378984224" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "1fd6b00b-d907-45c8-be40-05e130ddcee8" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984224" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "n5mKcf8+KAiqDsr6SxW11Lo8byFjp/RKocoHb/OabIOHFn7wdJWIcO9M7Tea4sMZ7rCKO9bt5YB/0SaPDQ3chA==" ,                    "signature": "ERV2dGrdl3OwwU60zbWiDBFFXRo627lEp9sMqwhjtXDtKq1d2Hb+NHVC5/jEDOJESBAW6lv/o+eOUmWchzkECg==" ,                    "timestamp": "1594378983483" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "55074c50-1c77-4045-833a-4671f41a7e98" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983483" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "mg85vlkthg+dShvpqDjs0PilEmb30iWRtSlgm2Bl2QJhB1bHpWc6Sjbpc/7M55vRxBt67A0ZEy3ozJurTDVEhw==" ,                    "signature": "waoFnm/OFRDOElwonR7dYTEJET13u59OxbJa1e/JbprqwM9LybogPS7nNeeT0kxIE9xiJ56lAvT35jks1+MqDg==" ,                    "timestamp": "1594378984200" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "d5c3270a-1335-4572-807d-b410062cc424" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984200" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "AUzzYipr0CElBJX+ROSTRK5RW4uynrxH+FzMsexoJyoJcGbMzFQn2z/ydVdrS5oXKBpfjHn9LsVSIUWv+Ry1Ew==" ,                    "signature": "g9Eyjyzs8H/Wxt791M2VBo9PsP0VfyPVRdvdP/OTsHZFJUv5MS6Obay6xOK3B4/3OQaoNizeiyaMiA0LrS93Aw==" ,                    "timestamp": "1594378983982" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "6e845cbc-55d6-484e-82c6-3d39191008c9" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983982" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "iAextzA3uY+WRbZDoOQo2zaD08ydtP1/AzNhTyQBhjMlRbIdTFx17IO3OLNWkFknH6A0ujl8lPJiH+6RN9JbSA==" ,                    "signature": "MFxz3IBgof5RyD3v15t7qoQ/qPODdXqWaL49Qzft6FwkZAgzPuTR6LVQjUnXZAIcoXrhU4q1fFWUr8QKsXdlCQ==" ,                    "timestamp": "1594378983889" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "3c96bc7b-6e8c-44ce-b905-4701252f6d95" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983889" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "X1d1E0DymH2xH/IFFOE/inbMP8XwQsYikDD6sqIDS0Vi978XodvEMTkHH5a0C6gW1w+2jLPotj5j7+tyff8uEA==" ,                    "signature": "B3SZJI3MG7Gfja494kC86w3P1foC/KcUGS7Dw+72vKC73fW5LLJk3T0JUpZcmsBGMqYkhF/lsyvq6gG7o+D0Cw==" ,                    "timestamp": "1594378983934" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "32f151fe-61c6-48cb-94b1-08ed09dd67d6" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983934" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "sSjWUvytRT6tBK5u2kIfjRbSYUAXGxyRFgcOfkowCXSKX5O+m6UKZ3tYBkXJL7MlKcQ9DajYKVm1SvmoOh/UIg==" ,                    "signature": "CcKZtspON/WC9eYGvLmdUkgW2BITQEvSB0evnO0KTFqyjAsiowX43V4OBQyVR/BJ1QPM4ELfc4abwtWcyglpCw==" ,                    "timestamp": "1594378984070" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "96cd2e80-403a-4385-96da-71ae82a9b958" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984070" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "OdJ6Dr2TeQlcitlYWsyW6/LCYDnFI6PNnv5dR9f2d70WBkCGIsYSGZq5HszzRmAUt0jvGILKZhaaYZXN9iQ/rg==" ,                    "signature": "SFkLlMkxl5rlD/REPHx+FvMgUXQL8ExS9w81APRf282P259BxlTOa3DniNrhVaFKCg89VCRsO0IGi/GGFNYQAA==" ,                    "timestamp": "1594378984261" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "fa81fca9-c2ce-4b54-a7c2-f13258c601ce" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984261" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "w3PeAbtraACm0uzu+OAS7uLiRP4HwTjgMD3PFQdvL+7b033m2675rUGcXRl4+P8+rb4EdRQOHJ7vSeTMmaN1Qg==" ,                    "signature": "gABbQDyhcVHI6u5mV5oruRpGY/s1jRLQQgwQEwCREo8O8bOc2GAGBuGragStRwwJ3f+210pxruHJDfgfPGUoAA==" ,                    "timestamp": "1594378984136" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "de51fb51-cfc1-4dce-9591-a7a0adf77efd" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984136" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "WaGej/rBZhmxai++TA3WWNGpJtkk0YB3zqtAZh2US4CWE4MfZeqjvdrTQT8CJGRE+4X/jOVxeQMHwSimyaQLxg==" ,                    "signature": "ZMg4yHgJuTwlVH5uXoD8wRw7SFG/EylE0dq5S7g0BQ554HVoHeJAjcssrPq6uAmCeScJ9iRF7hecCyPoviuzDw==" ,                    "timestamp": "1594378983856" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "c38d4524-68c7-40d6-8967-dfe919346e36" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983856" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "6J+k3xhKKZxeXujDz4cj22duxRniD7iccV6ovRkaa7QRYayAfuC8MU053zoB8zHzc6DmTmePd1kpmN0F8bbvYA==" ,                    "signature": "LjDx/tGoutdViJbIXzJYgHy5lk/WHEsHHA9KfoubGbB1AaoBJF8mTnDPU9wiWDXMtox+qGW1z6Fjb8o+9t0tCg==" ,                    "timestamp": "1594378984387" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "b4c6d66a-8743-48d3-a5f0-8b68618bb098" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984387" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "dBOtsKkIXAPU9gGgHqZD2yQS34JA+Nq2K6qg8pKcJSxH2hhz8rSNBspKhrhzP97A6u+ahO0/N/k3cpKdRFdAMg==" ,                    "signature": "rRI0+kpYQqRPGA0XAysqVpCLUEwLQNrub+lLHTuFdyENYsGqFfxQJx2wXfnzPJW7GY/KJ+hG2W3W6Sguj9BNDA==" ,                    "timestamp": "1594378984181" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "4962e07e-863d-4ea9-9f18-223b368b160e" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984181" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "NNevBoVqVtTNz0INi081b6aK3F7rVGv0zwkS8bU6P7nl+U2Kg3uXaxUuzBwMke0MwyaqgLJEs5lMF9J7s5kk+g==" ,                    "signature": "qSw4/rai6OZsFjvLWdcJJ99+UkCipPYIA7gW+uAq+4Esl0jsTU0uXtF+tcwS1hRv5TkcJm+W7WMKYOnItyTwDw==" ,                    "timestamp": "1594378984425" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "b48bbc3a-60f4-4147-a9f6-220980d1876c" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984425" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "DlGBWdeQkJiJrv0IfcmQ6K+umrBZRzg4IcU5ZJwXE1kZoFT1gLB+Q//JxiB5jNHiKUWPxppABriTPdEWmddGDw==" ,                    "signature": "A/Nl0ZRfrbKgecu6WzxkHXcZ7cKgamER6k7C25vVc4C+Q1iYelgSpV26iiQAwndTEoTGjm0l0II0h7aSj2LeDw==" ,                    "timestamp": "1594378984064" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "a1ba4dcb-cab5-4e20-896f-24696f11b72c" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984064" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "OuwgORre9UC3D9Fmi77xPNQsmQbRy/qd+hcmenDP3Zj9IncRaN+mc6G9i67U9udIO+nhYHxjA9jOXKhds/FCYQ==" ,                    "signature": "IqZ4TfajX+UU2ec2mq8ElQh0NEpW77Wiq0Um84ZcIJGLQGFmCBMmB547gWzjgTxp93R/xbaETJU5MElI+Ud2DQ==" ,                    "timestamp": "1594378984530" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "0366ab2c-28b5-4df8-85a3-1d94de959696" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984530" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "gnHHO7JUit7tXhdt/SzbzxsQL183E7wz5KQzPv90pJpx8Q7DA2H+tbPd0+r3UZG5X/HcSYPAjY4t98GM1JCx7g==" ,                    "signature": "Y5joRaphKUmBFZggh6Hgr30t6TNeePUvfuroiPW92vMMSOXW9tGLlzq4mUsLEcx5EJkBq4RnzewOWlp7NPbhAg==" ,                    "timestamp": "1594378983882" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "3e3057fb-27b1-475f-8575-83dabdab4ff0" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378983882" } } }, { "v_from": { "label": "UPP", "properties" : { "hash": "jLhtLrcdOFeuNB7GjlgX74ecuCUx3RzDaRJ7/uMOrhD3lAsFIl86+5ka2bgk4mOgTbi+EC/ew6XmvRC+t44N8Q==" ,                    "signature": "h2T0el0LcCwb7ZsThVio4z3E0U63c3il54CEJrcUCWvmIdUli5cIWQlstahfa1GENwbVa+vvX6+czxCTP77WDg==" ,                    "timestamp": "1594378984260" } }, "v_to": { "label": "DEVICE", "properties" : { "device_id": "e01a9abe-06c5-40c5-a4b9-a994243f6a39" } }, "edge": { "label": "UPP->DEVICE", "properties" : { "timestamp": "1594378984260" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "QqlYlDiMxJ2D/VIvK+tPeTyNYydun1Zf3lelxuk6gT7uZspfMqMn2+vwaPdj83IxTTtToLsio+jt76/kD61IvQ==" ,                    "signature": "oQ4GB4RX+CQpq5KvxAAU+Z5bpcxNR6UTIUdtg7hx1MRubDCGD8Nwv+Rs2U/HFFxyCLYmWUFgp4S4twf6W5B0Cw==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "Y9po4YC06n5W4ZgpKrw2iZwa/QTfgiUWvDjdkDjWpZf4Q/OSO6HM1nkTbMEGVnVkfIGyHElT6zSE8OQa2jCU1A==" ,                    "signature": "gxwgqfJrmuMqiZZ2Q9Uqsww1fPN5nrbS4wc9j99NyM6oB0v4sZGUNh6c2JsBsLE/eFGEHk2WHeF/DbrqQgsQDg==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "59l48gvhP8ntxMOHJEg6GkcmIME7Lx1akcqmeSIXAHops7syw+XoJkFy27bQ725iDOTz+52ve5VIDaHw+97nlw==" ,                    "signature": "pvSTlu/fPANzI4bharBXEOZ6H3lceek2eaWHHRMFRNXnFWVmLKC54gVaCWL/z29z90ZUTTyosh1+Vg9kc4NjCg==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "L67CDLyLcUeLbGyrTCVYJU7S2ieo1Y22b4/EfDPHBdqPp4TuvqXVdta+kgHGWYWrWR8cxV7MB+sCYIhZJgciwQ==" ,                    "signature": "frzbEd3xM0/wVxHZMCM0IxpayitxlpeIK2SL/rjzapVT5ep70yGpbDUw2k/b3auhW9GkQReWK5ScQMdTBSXUDw==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "jjGVjzkoCVUwmI2Uu/BYkfvh84K/vv3SXZFISMHUesiyPjxJE6zDODn5FZVqMUp3KeEjj8dA1nfah0AL6D1yRg==" ,                    "signature": "8blqBizqywXfnbqTGfqD+nNl4fkKvjKu7Bi8lyPvWlCt2UWnHz/IkVyYDlMLBrdLHzPcg1boi0g/ukmul7hkDQ==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "UPP", "properties" : { "hash": "rkhADevWr5SaySHCVIU0yXxPriyGsIRkVJ9s7P4rBw0wzZetL13Ov9GiG3jrsIjqahv0dE1Jd7IzHkIf4F55fQ==" ,                    "signature": "Sn7rqqbjp6iX/oKs4Nzl+LWwLkDliGGGs0aekYd95cUPNgOLX3mRd3kqPwlcf8ao1wqFI7Bv7K6AP0MiYf3jAg==" } }, "edge": { "label": "SLAVE_TREE->UPP", "properties" : { "timestamp": "1594378984010" } } }, { "v_from": { "label": "SLAVE_TREE", "properties" : { "hash": "f1f8807428db8516340794e9a095af4656681c66a015c89b62bb062843951a120f06fa1e6ac2365505126235339b633e932659e81bef5a4f5d608185c6bae0ee" ,                    "timestamp": "1594378984010" } }, "v_to": { "label": "SLAVE_TREE", "properties" : { "hash": "d9b085675ee25e9a9a454770c7049bd55d695f73a4061e20704af3008dfc8d9124016a4e84eb3745865d876f16e3cc784f2e8b277dfba7307c07f1c4054543b5" } }, "edge": { "label": "SLAVE_TREE->SLAVE_TREE", "properties" : { "timestamp": "1594378984010" } } } ]"""
      println(rel)
      val ds = Injector.get[AbstractDiscoveryService]
      val relationsParsed = ds.parseRelations(rel).get
      ds.store(relationsParsed)
      for (r <- relationsParsed) {
        if (!validateRelation(r, gc)) {
          logger.error(s"Failed for relation ${r.toString}")
          fail()
        }
      }
    }

    scenario("concrete case 2") {
      val Injector = FakeSimpleInjector("")
      val gc = Injector.get[GremlinConnector]
      implicit val propSet: Set[Property] = KafkaElements.propertiesToIterate
      val rel = """[ { "v_from": { "label": "MASTER_TREE", "properties": { "hash": "60e2806dd179973dfdb1311f9ce7677a7162e61e1a73328fde550245e2588d26feaf7fb83c2366542287f7bc41d25c8a87974fe61c10abe136e841715bbaf9d7" , "timestamp": 1594386051371 } }, "v_to": { "label": "SLAVE_TREE", "properties": { "hash": "cd0297cf9b9ac1a0d43458eaa332112a65e94d532c05386c5c19f91bb714e9352e17cc31f9a22658eb4d02839cca782c11aa2773120e5cd7daa56c96045b0c79" } }, "edge": { "label": "MASTER_TREE->SLAVE_TREE", "properties": { "timestamp": 1594386051371 } } }, { "v_from": { "label": "MASTER_TREE", "properties": { "hash": "60e2806dd179973dfdb1311f9ce7677a7162e61e1a73328fde550245e2588d26feaf7fb83c2366542287f7bc41d25c8a87974fe61c10abe136e841715bbaf9d7" , "timestamp": 1594386051371 } }, "v_to": { "label": "MASTER_TREE", "properties": { "hash": "cd0297cf9b9ac1a0d43458eaa332112a65e94d532c05386c5c19f91bb714e9352e17cc31f9a22658eb4d02839cca782c11aa2773120e5cd7daa56c96045b0c79" } }, "edge": { "label": "MASTER_TREE->MASTER_TREE", "properties": { "timestamp": 1594386051371 } } } ]"""
      println(rel)
      val ds = Injector.get[AbstractDiscoveryService]
      val relationsParsed = ds.parseRelations(rel).get
      ds.store(relationsParsed)
      for (r <- relationsParsed) {
        if (!validateRelation(r, gc)) {
          logger.error(s"Failed for relation ${r.toString}")
          fail()
        }
      }
    }

    scenario("try replicate bug") {
      val Injector = FakeSimpleInjector("")
      val gc = Injector.get[GremlinConnector]
      import scala.concurrent.ExecutionContext.Implicits.global

      gc.g.V().drop().iterate()

      def test() = {
        (1 to 10).toList.map { _ =>
          Future {
            try {
              //println("Add vertex!!!")
              gc.g.addV("UPP")
                .property(KeyValue[String](Key[String]("hash"), "valueHash"))
                .l()
              true
            } catch {
              case ex: Exception =>
                println(ex.getMessage)
                false
            }
          }
        }
      }

      (1 to 1000).foreach { i =>
        println("test: " + i)

        val fSeq = Future.sequence(test())
        import scala.concurrent.duration._

        println("Awaiting")
        Await.result(fSeq, 1.minute)

      }
      val count = gc.g.V().has(KeyValue(Key[String]("hash"), "valueHash")).l().size
      val v = gc.g.V().l()
      println(v.size)
      println(v.mkString(", "))
      println("Count of vertices: " + count)
      assert(count == 1, "Users > 1")

    }
  }

  def validateRelation(relation: Relation, gc: GremlinConnector)(implicit propSet: Set[Property]): Boolean = {
    validateVertex(relation.vFrom, gc) && validateVertex(relation.vTo, gc) && validateEdge(relation, gc)
  }

  def validateEdge(relation: Relation, gc: GremlinConnector)(implicit propSet: Set[Property]): Boolean = {
    val maybeVFrom = getVertexFromJg(relation.vFrom, gc)
    val maybeVTo = getVertexFromJg(relation.vTo, gc)
    maybeVFrom match {
      case Some(vFrom) =>
        maybeVTo match {
          case Some(vTo) =>
            var res = true
            val edge = gc.g.V(vFrom).outE().filter(_.inV().is(vTo)).l().head
            for (prop <- relation.edge.properties) {
              val propOnGraphValue = gc.g.E(edge).value(prop.keyValue.key).l().head
              if (prop.keyName == "timestamp") {
                val actualProp = propOnGraphValue.asInstanceOf[Date]
                val shouldBeDate = new Date(prop.value.toString.toLong)
                if (actualProp != shouldBeDate) res = false else {}
              } else {
                if (propOnGraphValue != prop.value.toString) res = false
              }
            }
            if (!res) logger.info("FALSE")
            res
          case None => false
        }
      case None => false
    }
  }

  def getVertexFromJg(vertexCore: VertexCore, gc: GremlinConnector)(implicit propSet: Set[Property]): Option[Vertex] = {
    vertexCore.properties.find(p => p.isUnique) match {
      case Some(uniqueProp) => gc.g.V().has(uniqueProp.toKeyValue).l().headOption
      case None => gc.g.V().has(vertexCore.properties.head.toKeyValue).l().headOption
    }
  }

  def validateVertex(vertexCore: VertexCore, gc: GremlinConnector)(implicit propSet: Set[Property]): Boolean = {
    val maybeVertex = getVertexFromJg(vertexCore, gc)
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
        res
      case None => false
    }

  }

  /**
    * Simple injector that replaces the kafka bootstrap server and topics to the given ones
    */
  def FakeSimpleInjector(bootstrapServers: String, port: Int = 8182): InjectorHelper = new InjectorHelper(List(new Binder {
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
