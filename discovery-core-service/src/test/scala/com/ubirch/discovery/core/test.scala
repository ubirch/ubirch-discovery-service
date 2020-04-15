package com.ubirch.discovery.core

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import com.ubirch.discovery.core.structure.VertexStruct
import gremlin.scala.{ Key, Vertex }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

object test extends LazyLogging {

  val queryLabel = "FOUNDATION_TREE"

  implicit val ec: ExecutionContext = ExecutionContextHelper.ec

  def main(args: Array[String]): Unit = {
    val res = find("hash", "27cuYVCzDh8WdFa58ruyHnGA6qCoU4Ap", queryLabel)
  }

  def find(key: String, value: String, label: String): Future[List[String]] = {
    implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

    val res: List[Vertex] = gc.g.V().has(Key[String](key), value).hasLabel("UPP").repeat(_.out().simplePath()).until(_.hasLabel(label)).path().limit(1).unfold().l().asInstanceOf[List[Vertex]]
    // val res2 = gc.g.V().has(Key[String]("hash"), "mssitgVtQ8MFhBFvuJPTEx9E97iNJYgW").repeat(_.out().simplePath()).until(_.hasLabel("PUBLIC_CHAIN")).path().limit(1).unfold().valueMap().l()
    val lastMasterTree = res(res.length - 2)
    val treu = res.last
    logger.info(gc.g.V(treu).label().l().head)
    logger.info(gc.g.V(treu).valueMap().l().head.asScala.toMap.mkString(", "))
    logger.info(gc.g.V(lastMasterTree).label().l().head)
    logger.info(gc.g.V(lastMasterTree).valueMap().l().head.asScala.toMap.mkString(", "))
    val blockchainList = gc.g.V(lastMasterTree).out().hasLabel(label).l()

    val listPath = res.take(res.length - 1) ++ blockchainList

    def getFullPathJson(vList: List[Vertex]) = {
      vList map { v =>
        val valueMap = gc.g.V(v).valueMap.l().head.asScala.toMap map { x => x._1.toString -> x._2.asInstanceOf[java.util.ArrayList[String]].get(0) }
        val label = gc.g.V(v).label().l().head
        VertexStruct(label, valueMap)
      }
    }
    val t0 = System.currentTimeMillis()
    val t = getFullPathJson(listPath)
    logger.info("path: " + t.map { s => s.toJson })
    logger.info("len path: " + t.length)

    logger.info("path unsorted: " + getFullPathJson(res).map { s => s.toJson })

    logger.info("time: " + (System.currentTimeMillis() - t0).toString + " ms")
    Future(t.map { s => s.toJson })
  }
}
