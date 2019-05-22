package com.ubirch.discovery.core

import com.ubirch.discovery.core.structure.VertexStructDb
import gremlin.scala.{Key, KeyValue}

class AddVertices(implicit val gc: GremlinConnector) {

  private val label = "aLabel"

  private val ID: Key[String] = Key[String]("IdAssigned")

  def addTwoVertices(id1: String, p1: List[KeyValue[String]], l1: String = label)
                    (id2: String, p2: List[KeyValue[String]], l2: String = label)
                    (pE: List[KeyValue[String]]): String = {
    if (id1 == id2) throw new IllegalArgumentException("id1 should not be equal to id2")
    val v1: VertexStructDb = new VertexStructDb(id1, gc.g)
    val v2: VertexStructDb = new VertexStructDb(id2, gc.g)
    howMany(v1, v2) match {
      case 0 => noneExist(v1, p1, l1)(v2, p2, l2)(pE)
      case 1 => oneExist(v1, p1, l1)(v2, p2, l2)(pE)
      case 2 => twoExist(v1, v2, pE)
    }
    "OK BB"
  }

  private def noneExist(v1: VertexStructDb, p1: List[KeyValue[String]], l1: String)
                       (v2: VertexStructDb,  p2: List[KeyValue[String]], l2: String)
                       (pE: List[KeyValue[String]]): Unit = {
    v1.addVertex(p1, l1, gc.b)
    v2.addVertex(p2, l2, gc.b)
    createEdge(v1, v2, pE)
  }

  private def oneExist(v1: VertexStructDb,  p1: List[KeyValue[String]], l1: String)
                      (v2: VertexStructDb,  p2: List[KeyValue[String]], l2: String)
                      (pE: List[KeyValue[String]]): Unit = {
    if (v1.exist) {
      v2.addVertex(p2, l2, gc.b)
      createEdge(v1, v2, pE)
    } else {
      v1.addVertex(p1, l1, gc.b)
      createEdge(v1, v2, pE)
    }
  }

  private def twoExist(v1: VertexStructDb, v2: VertexStructDb, pE: List[KeyValue[String]]): Unit = {
    if (!areVertexLinked(v1, v2)) createEdge(v1, v2, pE)
  }

  private def howMany(v1: VertexStructDb, v2: VertexStructDb): Int = {
    if (v1.exist) {
      if (v2.exist) 2 else 1
    } else if (v2.exist) 1 else 0
  }

  private def createEdge(v1: VertexStructDb, v2: VertexStructDb, pE: List[KeyValue[String]]): Unit = {
    val edge = gc.g.V(v1.vertex).as("a").V(v2.vertex).addE("aLabel").from(v1.vertex).toSet().head //v1.vertex --- "alabel" --> v2.vertex.element
    for (keyV <- pE) {
      gc.g.E(edge).property(keyV).iterate()
    }
  }

  private def areVertexLinked(v1: VertexStructDb, v2: VertexStructDb): Boolean = {
    val oneWay = gc.g.V(v1.vertex).outE().as("e").inV.has(ID, v2.id).select("e").toList
    val otherWay = gc.g.V(v2.vertex).outE().as("e").inV.has(ID, v1.id).select("e").toList
    oneWay.nonEmpty || otherWay.nonEmpty
  }

}
