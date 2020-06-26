package com.ubirch.discovery.services.connector

import java.util.Date

import gremlin.scala._
import javax.inject.Singleton
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.apache.tinkerpop.gremlin.structure.{ Direction, Vertex }
import org.janusgraph.core.{ JanusGraph, JanusGraphFactory }
import org.janusgraph.core.Cardinality._
import org.janusgraph.core.Multiplicity._
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.graphdb.database.management.ManagementSystem

import scala.language.postfixOps
import scala.util.Try

@Singleton
class JanusGraphForTests extends GremlinConnector {

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
    conf.addProperty("storage.backend", "inmemory")
    conf.addProperty("storage.directory", "/tmp/db/berkeleyje")
    conf.addProperty("storage.transactions", false)

    conf
  }

  def addSchema(): Unit = {
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
    val idx_timestamp = mgmt.buildIndex("indexTimestamp", classOf[Vertex])

    val timestamp = mgmt.getPropertyKey("timestamp")
    val eLabelDeviceUpp = mgmt.getEdgeLabel("UPP->DEVICE")

    idx_signature.addKey(mgmt.getPropertyKey("signature")).buildCompositeIndex()
    idx_type.addKey(mgmt.getPropertyKey("type")).buildCompositeIndex()
    idx_blockchain.addKey(mgmt.getPropertyKey("blockchain_type")).buildCompositeIndex()
    idx_hash.addKey(mgmt.getPropertyKey("hash")).unique().buildCompositeIndex()
    idx_device_id.addKey(mgmt.getPropertyKey("device_id")).unique().buildCompositeIndex()
    idx_transaction_id.addKey(mgmt.getPropertyKey("transaction_id")).unique().buildCompositeIndex()
    idx_timestamp.addKey(mgmt.getPropertyKey("timestamp")).buildCompositeIndex()

    mgmt.buildEdgeIndex(eLabelDeviceUpp, "indexEdgeUppDevice", Direction.IN, timestamp)

    mgmt.commit()

    ManagementSystem.awaitGraphIndexStatus(jg, "indexSignature").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexType").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexHash").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexDeviceId").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexBlockchain").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexTransactionId").call()
    ManagementSystem.awaitGraphIndexStatus(jg, "indexTimestamp").call()

    ManagementSystem.awaitRelationIndexStatus(jg, "indexEdgeUppDevice", "UPP->DEVICE").call()

    mgmt = jg.openManagement()
    mgmt.updateIndex(mgmt.getGraphIndex("indexSignature"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexType"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexHash"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexDeviceId"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexBlockchain"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexTransactionId"), SchemaAction.REINDEX).get()
    mgmt.updateIndex(mgmt.getGraphIndex("indexTimestamp"), SchemaAction.REINDEX).get()

    mgmt.updateIndex(mgmt.getRelationIndex(eLabelDeviceUpp, "indexEdgeUppDevice"), SchemaAction.REINDEX).get()

    mgmt.commit()

  }

}
