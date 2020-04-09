package com.ubirch.discovery.core

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddRelation
import com.ubirch.discovery.core.structure._
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

  val KEY_HASH: Key[Any] = Key[Any]("hash")
  val KEY_BC: Key[Any] = Key[Any]("blockchain_type")
  val KEY_TIMESTAMP: Key[Any] = Key[Any]("timestamp")
  val edgeProps = List(ElementProperty(KeyValue(KEY_HASH, "blabla"), PropertyType.String))

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
    val internalDevice = VertexCore(
      List(ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String), getTimestampProp), "DEVICE"
    )
    val device = new VertexDatabase(internalDevice, gc)
    device.addVertexWithProperties()
    device
  }

  def initUPP(device: VertexDatabase, number: Int): List[VertexDatabase] = {
    var v = new ListBuffer[VertexDatabase]
    for (_ <- 0 until number) {
      val UPPprops = List(
        ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String),
        getTimestampProp
      )
      val internalUPP = VertexCore(UPPprops, "UPP")
      AddRelation.createRelationOneCached(device)(internalUPP)(EdgeCore(edgeProps, "DEVICE->UPP"))
      v += internalUPP.toVertexStructDb(gc)
    }
    v.toList
  }

  def initFT(FT: VertexDatabase = null, UPPs: List[VertexDatabase]): VertexDatabase = {
    val newFTprops = List(
      ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String),
      getTimestampProp
    )
    if (FT == null) {
      for (i <- 0 to 3) {
        AddRelation.createRelationOneCached(UPPs(i))(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "UPP->FT"))
      }
    } else {
      AddRelation.createRelationOneCached(FT)(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "FT->FT"))
      for (i <- 0 to 2) {
        AddRelation.createRelationOneCached(UPPs(i))(VertexCore(newFTprops, "FOUNDATION_TREE"))(EdgeCore(edgeProps, "UPP->FT"))
      }
    }
    new VertexDatabase(VertexCore(newFTprops, "FOUNDATION_TREE"), gc)
  }

  def initMT(MT: VertexDatabase = null, FTs: List[VertexDatabase]): VertexDatabase = {
    val newMTprops = List(
      ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String),
      getTimestampProp
    )
    if (MT == null) {
      for (i <- 0 to 3) {
        AddRelation.createRelationOneCached(FTs(i))(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "FT->MT"))
      }
    } else {
      AddRelation.createRelationOneCached(MT)(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "MT->MT"))
      for (i <- 0 to 2) {
        AddRelation.createRelationOneCached(FTs(i))(VertexCore(newMTprops, "MASTER_TREE"))(EdgeCore(edgeProps, "FT->MT"))
      }
    }
    new VertexDatabase(VertexCore(newMTprops, "MASTER_TREE"), gc)
  }

  def initBcx(MT: VertexDatabase): String = {
    val iotaProps = List(
      ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String),
      ElementProperty(KeyValue(KEY_BC, "IOTA"), PropertyType.String),
      getTimestampProp
    )
    val ethProps = List(
      ElementProperty(KeyValue(KEY_HASH, Random.alphanumeric.take(32).mkString), PropertyType.String),
      ElementProperty(KeyValue(KEY_BC, "ETH"), PropertyType.String),
      getTimestampProp
    )
    AddRelation.createRelationOneCached(MT)(VertexCore(iotaProps, "PUBLIC_CHAIN"))(EdgeCore(edgeProps, "MT->BCX"))
    AddRelation.createRelationOneCached(MT)(VertexCore(ethProps, "PUBLIC_CHAIN"))(EdgeCore(edgeProps, "MT->BCX"))
  }

  def getTimestampProp = ElementProperty(KeyValue(KEY_TIMESTAMP, getTime), PropertyType.Long)
  def getTime = System.currentTimeMillis()

}
