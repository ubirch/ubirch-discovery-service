package com.ubirch.discovery.services.connector

import java.util.Date

import gremlin.scala._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.{ Direction, Vertex }
import org.janusgraph.core.Cardinality._
import org.janusgraph.core.Multiplicity._
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.core.{ JanusGraph, JanusGraphFactory }
import org.janusgraph.graphdb.database.management.ManagementSystem

import scala.language.postfixOps
import scala.util.Try

protected class JanusGraphForTests extends GremlinConnector {

  val jg: JanusGraph = JanusGraphFactory.open(buildTestProperties)
  implicit val graph: ScalaGraph = jg.asScala
  val g: TraversalSource = graph.traversal
  val b: Bindings = Bindings.instance

  addSchema()

  override def closeConnection(): Unit = graph.close()

  /**
    * Create the properties necessary to make an embedded JanusGraph run
    * Uses berkeleyje and lucene instead of cassandra and ES and they're light storage and backend index that can run
    * embedded without issues
    */
  def buildTestProperties: PropertiesConfiguration = {

    import sys.process._
    Try("rm -rf /tmp/db/berkeleyje" !) // delete previous graph database stored here to start with a fresh graph

    val conf = new PropertiesConfiguration()

    conf.addProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory")
    conf.addProperty("storage.backend", "berkeleyje")
    conf.addProperty("storage.directory", "/tmp/db/berkeleyje")
    conf.addProperty("index.search.backend", "lucene")
    conf.addProperty("index.search.directory", "/tmp/db/lucene")

    conf
  }

  def addSchema() = {
    graph.tx().rollback()
    var mgmt = jg.openManagement()

    mgmt.makeEdgeLabel("UPP->DEVICE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("SLAVE_TREE->UPP").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("SLAVE_TREE->SLAVE_TREE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("MASTER_TREE->SLAVE_TREE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("MASTER_TREE->MASTER_TREE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("MASTER_TREE_UPGRADE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("PUBLIC_CHAIN->MASTER_TREE").multiplicity(SIMPLE).make()
    mgmt.makeEdgeLabel("CHAIN").multiplicity(SIMPLE).make()

    mgmt.makePropertyKey("signature").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("type").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("hash").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("device_id").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("blockchain_type").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("transaction_id").dataType(classOf[String]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("timestamp").dataType(classOf[Date]).cardinality(SINGLE).make()
    mgmt.makePropertyKey("producer_id").dataType(classOf[String]).cardinality(SINGLE).make()

    mgmt.commit()

    mgmt = jg.openManagement()
    val idx_signature = mgmt.buildIndex("indexSignature", classOf[Vertex])
    val idx_type = mgmt.buildIndex("indexType", classOf[Vertex])
    val idx_hash = mgmt.buildIndex("indexHash", classOf[Vertex])
    val idx_device_id = mgmt.buildIndex("indexDeviceId", classOf[Vertex])
    val idx_blockchain = mgmt.buildIndex("indexBlockchain", classOf[Vertex])
    val idx_transaction_id = mgmt.buildIndex("indexTransactionId", classOf[Vertex])
    val idx_timestamp_and_producer = mgmt.buildIndex("indexTimestampAndOwner", classOf[Vertex])

    val timestamp = mgmt.getPropertyKey("timestamp")
    val eLabelDeviceUpp = mgmt.getEdgeLabel("UPP->DEVICE")
    val eLabelSlaveUpp = mgmt.getEdgeLabel("SLAVE_TREE->UPP")
    val eLabelChain = mgmt.getEdgeLabel("CHAIN")
    val eLabelSlaveSlave = mgmt.getEdgeLabel("SLAVE_TREE->SLAVE_TREE")
    val eLabelMasterSlave = mgmt.getEdgeLabel("MASTER_TREE->SLAVE_TREE")
    val eLabelMasterMaster = mgmt.getEdgeLabel("MASTER_TREE->MASTER_TREE")
    val eLabelMasterUpgrade = mgmt.getEdgeLabel("MASTER_TREE_UPGRADE")
    val eLabelBlockchainMaster = mgmt.getEdgeLabel("PUBLIC_CHAIN->MASTER_TREE")

    idx_signature.addKey(mgmt.getPropertyKey("signature")).buildCompositeIndex()
    idx_type.addKey(mgmt.getPropertyKey("type")).buildCompositeIndex()
    idx_blockchain.addKey(mgmt.getPropertyKey("blockchain_type")).buildCompositeIndex()
    idx_hash.addKey(mgmt.getPropertyKey("hash")).unique().buildCompositeIndex()
    idx_device_id.addKey(mgmt.getPropertyKey("device_id")).unique().buildCompositeIndex()
    idx_transaction_id.addKey(mgmt.getPropertyKey("transaction_id")).unique().buildCompositeIndex()
    idx_timestamp_and_producer.addKey(mgmt.getPropertyKey("timestamp")).addKey(mgmt.getPropertyKey("producer_id")).buildMixedIndex("search")

    val idx_edge_UPP_DEVICE = mgmt.buildEdgeIndex(eLabelDeviceUpp, "indexEdgeUppDevice", Direction.IN, timestamp)
    val idx_edge_CHAIN = mgmt.buildEdgeIndex(eLabelChain, "indexEdgeChain", Direction.BOTH, timestamp)
    val idx_edge_SLAVE_UPP = mgmt.buildEdgeIndex(eLabelSlaveUpp, "indexEdgeSlaveUpp", Direction.BOTH, timestamp)
    val idx_edge_SLAVE_SLAVE = mgmt.buildEdgeIndex(eLabelSlaveSlave, "indexEdgeSlaveSlave", Direction.BOTH, timestamp)
    val idx_edge_MASTER_SLAVE = mgmt.buildEdgeIndex(eLabelMasterSlave, "indexEdgeMasterSlave", Direction.BOTH, timestamp)
    val idx_edge_MASTER_MASTER = mgmt.buildEdgeIndex(eLabelMasterMaster, "indexEdgeMasterMaster", Direction.BOTH, timestamp)
    val idx_edge_UPGRADE = mgmt.buildEdgeIndex(eLabelMasterUpgrade, "indexEdgeMasterUpgrade", Direction.BOTH, timestamp)
    val idx_edge_BLOCKCHAIN_MASTER = mgmt.buildEdgeIndex(eLabelBlockchainMaster, "indexEdgeBlockchainMaster", Direction.BOTH, timestamp)

    mgmt.commit()

    ManagementSystem.awaitGraphIndexStatus(jg, "indexSignature").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexType").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexHash").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexDeviceId").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexBlockchain").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexTransactionId").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexTimestampAndOwner").call()

    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeUppDevice", "UPP->DEVICE").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeChain", "CHAIN").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeSlaveUpp", "SLAVE_TREE->UPP").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeSlaveSlave", "SLAVE_TREE->SLAVE_TREE").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeMasterMaster", "MASTER_TREE->MASTER_TREE").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeMasterUpgrade", "MASTER_TREE_UPGRADE").call()
    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeBlockchainMaster", "PUBLIC_CHAIN->MASTER_TREE").call()

    mgmt = jg.openManagement()
    mgmt.updateIndex(mgmt.getGraphIndex("indexSignature"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexType"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexHash"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexDeviceId"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexBlockchain"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexTransactionId"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexTimestampAndOwner"), SchemaAction.REINDEX).get()

    mgmt.updateIndex(mgmt.getRelationIndex(eLabelDeviceUpp, "indexEdgeUppDevice"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelChain, "indexEdgeChain"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelSlaveUpp, "indexEdgeSlaveUpp"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelSlaveSlave, "indexEdgeSlaveSlave"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelMasterSlave, "indexEdgeMasterSlave"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelMasterMaster, "indexEdgeMasterMaster"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelMasterUpgrade, "indexEdgeMasterUpgrade"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getRelationIndex(eLabelBlockchainMaster, "indexEdgeBlockchainMaster"), SchemaAction.REINDEX).get()

    mgmt.commit()

  }

}
