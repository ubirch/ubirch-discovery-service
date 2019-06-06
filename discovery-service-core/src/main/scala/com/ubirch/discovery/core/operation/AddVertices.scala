package com.ubirch.discovery.core.operation

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.{ ImportToGremlinException, KeyNotInList }
import com.ubirch.discovery.core.util.Util.{ extractValue, getEdge, getEdgeProperties, recompose }
import gremlin.scala.{ Key, KeyValue }

import scala.language.postfixOps

case class AddVertices()(implicit gc: GremlinConnector) extends LazyLogging {

  private val label = "aLabel"

  private val ID: Key[String] = Key[String]("IdAssigned")

  def addTwoVertices(id1: String, p1: List[KeyValue[String]], l1: String = label)
    (id2: String, p2: List[KeyValue[String]], l2: String = label)
    (pE: List[KeyValue[String]], lE: String = label): String = {
    if (id1 == id2) throw new IllegalArgumentException("id1 should not be equal to id2")
    val v1: VertexStructDb = new VertexStructDb(id1, gc.g)
    val v2: VertexStructDb = new VertexStructDb(id2, gc.g)
    howMany(v1, v2) match {
      case 0 => noneExist(v1, p1, l1)(v2, p2, l2)(pE, lE)
      case 1 => oneExist(v1, p1, l1)(v2, p2, l2)(pE, lE)
      case 2 => twoExist(v1, v2, pE, lE)
    }
    "OK BB"
  }

  private def noneExist(v1: VertexStructDb, p1: List[KeyValue[String]], l1: String)
    (v2: VertexStructDb, p2: List[KeyValue[String]], l2: String)
    (pE: List[KeyValue[String]], lE: String): Unit = {
    v1.addVertex(p1, l1, gc.b)
    verifVertex(v1, p1, l1)
    v2.addVertex(p2, l2, gc.b)
    verifVertex(v2, p2, l2)
    createEdge(v1, v2, pE, lE)
    verifEdge(v1.id, v2.id, pE)
  }

  private def oneExist(v1: VertexStructDb, p1: List[KeyValue[String]], l1: String)
    (v2: VertexStructDb, p2: List[KeyValue[String]], l2: String)
    (pE: List[KeyValue[String]], lE: String): Unit = {
    if (v1.exist) {
      v2.addVertex(p2, l2, gc.b)
      verifVertex(v2, p2, l2)
      createEdge(v1, v2, pE, lE)
      verifEdge(v1.id, v2.id, pE)
    } else {
      v1.addVertex(p1, l1, gc.b)
      verifVertex(v1, p1, l1)
      createEdge(v1, v2, pE, lE)
      verifEdge(v1.id, v2.id, pE)
    }
  }

  private def twoExist(v1: VertexStructDb, v2: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    if (!areVertexLinked(v1, v2)) {
      createEdge(v1, v2, pE, lE)
      verifEdge(v1.id, v2.id, pE)
    }
  }

  private def howMany(v1: VertexStructDb, v2: VertexStructDb): Int = {
    if (v1.exist) {
      if (v2.exist) 2 else 1
    } else if (v2.exist) 1 else 0
  }

  private def createEdge(v1: VertexStructDb, v2: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    val edge = gc.g.V(v1.vertex).as("a").V(v2.vertex).addE(lE).from(v1.vertex).toSet().head //v1.vertex --- "alabel" --> v2.vertex.element
    for (keyV <- pE) {
      gc.g.E(edge).property(keyV).iterate()
    }
  }

  private def areVertexLinked(v1: VertexStructDb, v2: VertexStructDb): Boolean = {
    val oneWay = gc.g.V(v1.vertex).outE().as("e").inV.has(ID, v2.id).select("e").toList
    val otherWay = gc.g.V(v2.vertex).outE().as("e").inV.has(ID, v1.id).select("e").toList
    oneWay.nonEmpty || otherWay.nonEmpty
  }

  def verifVertex(vertexStruct: VertexStructDb, properties: List[KeyValue[String]], l: String = label): Unit = {
    if (!vertexStruct.exist) throw new ImportToGremlinException("Vertex wasn't imported to the Gremlin Server")

    val keyList: Array[Key[String]] = properties.map(x => x.key).toArray :+ ID
    val propertiesInServer = vertexStruct.getPropertiesMap
    val idInServer = extractValue[String](propertiesInServer, ID.name)
    val propertiesInServerAsListKV = try {
      recompose(propertiesInServer, keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with id = $idInServer wasn't correctly imported to the database: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServerAsListKV.sortBy(x => x.key.name) == properties.sortBy(x => x.key.name)))
      throw new ImportToGremlinException(s"Vertex with id = $idInServer wasn't correctly imported to the database: properties are not correct")
    if (!idInServer.equals(vertexStruct.id))
      throw new ImportToGremlinException(s"Vertex with id = ${vertexStruct.id} wasn't correctly imported to the database: id is not the same")
  }

  def verifEdge(idFrom: String, idTo: String, properties: List[KeyValue[String]]): Unit = {
    val edge = getEdge(gc, idFrom, idTo, ID).head

    if (edge == null) throw new ImportToGremlinException(s"Edge between $idFrom and $idTo wasn't created")
    val keyList = properties map (x => x.key) toArray
    val propertiesInServer = try {
      recompose(getEdgeProperties(gc, edge), keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Edge between $idFrom and $idTo wasn't correctly created: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServer.sortBy(x => x.key.name) == properties.sortBy(x => x.key.name)))
      throw new ImportToGremlinException(s"Edge between $idFrom and $idTo wasn't correctly created: properties are not correct")
  }

}
