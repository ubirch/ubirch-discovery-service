package com.ubirch.discovery.core

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.{EdgeToAdd, VertexStructDb, VertexToAdd}
import com.ubirch.discovery.core.structure.Elements.Property
import gremlin.scala.{Key, KeyValue}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object PopulateNewStruct extends LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

  case object SIGNATURE extends Property("signature", true)
  case object TYPE extends Property("type")
  case object BLOCKCHAIN_TYPE extends Property("blockchain")
  case object HASH extends Property("hash", true)
  case object DEVICE_ID extends Property("device_id", true)

  val KEY_HASH: Key[String] = Key[String]("hash")
  val KEY_BC: Key[String] = Key[String]("blockchain_type")
  val edgeProps = List(KeyValue(KEY_HASH, "blabla"))

  implicit val propSet: Set[Property] = Set(SIGNATURE, HASH, DEVICE_ID)

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  def main(args: Array[String]): Unit = {
    deleteDatabase()
    val device = initDevice()
    var UPPs = new ListBuffer[VertexStructDb]
    UPPs ++= initUPP(device, 4)

    var FTs = new ListBuffer[VertexStructDb]
    FTs += initFT(null, UPPs.toList)

    var MTs = new ListBuffer[VertexStructDb]

    for (i <- 0 to 1000) {
      UPPs ++= initUPP(device, 3)
      FTs += initFT(FTs.toList.last, UPPs.toList.takeRight(3))
      if (i % 4 == 0 && i != 0) {
        MTs += (if (MTs.isEmpty) initMT(null, FTs.toList.take(4)) else initMT(MTs.toList.last, FTs.toList.takeRight(3)))
      }
      if (i % 50 == 0 && i != 0) {
        initBcx(MTs.toList.last)
      }
    }
    println("finished!")
    logger.info("finished!")
  }

  def initDevice(): VertexStructDb = {
    val internalDevice = VertexToAdd(List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString)), "DEVICE")
    val device = new VertexStructDb(internalDevice, gc.g)
    device.addVertexWithProperties(gc.b)
    device
  }

  def initUPP(device: VertexStructDb, number: Int): List[VertexStructDb] = {
    var v = new ListBuffer[VertexStructDb]
    for (_ <- 0 until number) {
      val UPPprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
      val internalUPP = VertexToAdd(UPPprops, "UPP")
      AddVertices().addTwoVerticesCached(device)(internalUPP)(EdgeToAdd(edgeProps, "DEVICE->UPP"))
      v += internalUPP.toVertexStructDb(gc.g)
    }
    v.toList
  }

  def initFT(FT: VertexStructDb = null, UPPs: List[VertexStructDb]): VertexStructDb = {
    val newFTprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
    if (FT == null) {
      for (i <- 0 to 3) {
        AddVertices().addTwoVerticesCached(UPPs(i))(VertexToAdd(newFTprops, "FOUNDATION_TREE"))(EdgeToAdd(edgeProps, "UPP->FT"))
      }
    } else {
      AddVertices().addTwoVerticesCached(FT)(VertexToAdd(newFTprops, "FOUNDATION_TREE"))(EdgeToAdd(edgeProps, "FT->FT"))
      for (i <- 0 to 2) {
        AddVertices().addTwoVerticesCached(UPPs(i))(VertexToAdd(newFTprops, "FOUNDATION_TREE"))(EdgeToAdd(edgeProps, "UPP->FT"))
      }
    }
    new VertexStructDb(VertexToAdd(newFTprops, "FOUNDATION_TREE"), gc.g)
  }

  def initMT(MT: VertexStructDb = null, FTs: List[VertexStructDb]): VertexStructDb = {
    val newMTprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
    if (MT == null) {
      for (i <- 0 to 3) {
        AddVertices().addTwoVerticesCached(FTs(i))(VertexToAdd(newMTprops, "MASTER_TREE"))(EdgeToAdd(edgeProps, "FT->MT"))
      }
    } else {
      AddVertices().addTwoVerticesCached(MT)(VertexToAdd(newMTprops, "MASTER_TREE"))(EdgeToAdd(edgeProps, "MT->MT"))
      for (i <- 0 to 2) {
        AddVertices().addTwoVerticesCached(FTs(i))(VertexToAdd(newMTprops, "MASTER_TREE"))(EdgeToAdd(edgeProps, "FT->MT"))
      }
    }
    new VertexStructDb(VertexToAdd(newMTprops, "MASTER_TREE"), gc.g)
  }

  def initBcx(MT: VertexStructDb) = {
    val iotaProps = List(
      KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString),
      KeyValue(KEY_BC, "IOTA")
    )
    val ethProps = List(
      KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString),
      KeyValue(KEY_BC, "ETH")
    )
    AddVertices().addTwoVerticesCached(MT)(VertexToAdd(iotaProps, "PUBLIC_CHAIN"))(EdgeToAdd(edgeProps, "MT->BCX"))
    AddVertices().addTwoVerticesCached(MT)(VertexToAdd(ethProps, "PUBLIC_CHAIN"))(EdgeToAdd(edgeProps, "MT->BCX"))
  }
}
