package com.ubirch.discovery.core

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure.{EdgeCore, VertexCore, VertexDatabase}
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
    var UPPs = new ListBuffer[VertexDatabase]
    UPPs ++= initUPP(device, 4)

    var FTs = new ListBuffer[VertexDatabase]
    FTs += initFT(null, UPPs.toList)

    var MTs = new ListBuffer[VertexDatabase]

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

  def initDevice(): VertexDatabase = {
    val internalDevice = VertexCore(List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString)), "DEVICE")
    val device = new VertexDatabase(internalDevice, gc)
    device.addVertexWithProperties()
    device
  }

  def initUPP(device: VertexDatabase, number: Int): List[VertexDatabase] = {
    var v = new ListBuffer[VertexDatabase]
    for (_ <- 0 until number) {
      val UPPprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
      val internalUPP = VertexCore(UPPprops, "UPP")
      AddRelation().addTwoVerticesCached(device)(internalUPP)(EdgeCore(edgeProps, "DEVICE->UPP"))
      v += internalUPP.toVertexStructDb(gc)
    }
    v.toList
  }

  def initFT(FT: VertexDatabase = null, UPPs: List[VertexDatabase]): VertexDatabase = {
    val newFTprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
    if (FT == null) {
      for (i <- 0 to 3) {
        AddRelation().addTwoVerticesCached(UPPs(i))(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "UPP->FT"))
      }
    } else {
      AddRelation().addTwoVerticesCached(FT)(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "FT->FT"))
      for (i <- 0 to 2) {
        AddRelation().addTwoVerticesCached(UPPs(i))(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "UPP->FT"))
      }
    }
    new VertexDatabase(VertexCore(newFTprops, "FOUNDATION_TREE"), gc)
  }

  def initMT(MT: VertexDatabase = null, FTs: List[VertexDatabase]): VertexDatabase = {
    val newMTprops = List(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString))
    if (MT == null) {
      for (i <- 0 to 3) {
        AddRelation().addTwoVerticesCached(FTs(i))(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "FT->MT"))
      }
    } else {
      AddRelation().addTwoVerticesCached(MT)(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "MT->MT"))
      for (i <- 0 to 2) {
        AddRelation().addTwoVerticesCached(FTs(i))(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "FT->MT"))
      }
    }
    new VertexDatabase(VertexCore(newMTprops, "MASTER_TREE"), gc)
  }

  def initBcx(MT: VertexDatabase) = {
    val iotaProps = List(
      KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString),
      KeyValue(KEY_BC, "IOTA")
    )
    val ethProps = List(
      KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString),
      KeyValue(KEY_BC, "ETH")
    )
    AddRelation().addTwoVerticesCached(MT)(VertexCore(iotaProps, "PUBLIC_CHAIN"))(EdgeCore(edgeProps, "MT->BCX"))
    AddRelation().addTwoVerticesCached(MT)(VertexCore(ethProps, "PUBLIC_CHAIN"))(EdgeCore(edgeProps, "MT->BCX"))
  }
}
