package com.ubirch.discovery.core.operation

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.{IdNotCorrect, ImportToGremlinException, KeyNotInList}
import com.ubirch.discovery.core.util.Util.{extractValue, getEdge, getEdgeProperties, recompose}
import gremlin.scala.{Key, KeyValue}

import scala.language.postfixOps

/**
  * Allows the storage of two nodes (vertices) in the janusgraph server. Link them together
  *
  * @param gc A GremlinConnector connected to a janusgraph server
  */
case class AddVertices()(implicit gc: GremlinConnector) extends LazyLogging {

  private val label = "aLabel"

  private val ID: Key[String] = Key[String]("IdAssigned")

  /* main part of the program */
  def addTwoVertices(id1: String, p1: List[KeyValue[String]], l1: String = label)
    (id2: String, p2: List[KeyValue[String]], l2: String = label)
    (pE: List[KeyValue[String]], lE: String = label): String = {
    logger.info(s"operating on two vertices: one $l1 and one $l2")
    val t0 = System.nanoTime()
    if (id1 == id2) throw IdNotCorrect(s"id1 $id1 should not be equal to id2")
    val vFrom: VertexStructDb = new VertexStructDb(id1, gc.g)
    val vTo: VertexStructDb = new VertexStructDb(id2, gc.g)
    howMany(vFrom, vTo) match {
      case 0 => noneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 1 => oneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 2 => twoExist(vFrom, vTo, pE, lE)
    }
    val t1 = System.nanoTime()
    logger.info(s"message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "OK BB" //TODO: change this return line
  }

  def addTwoVerticesCached(vCached: VertexStructDb)
                          (idOther: String, pOther: List[KeyValue[String]], lOther: String = label)
                          (pE: List[KeyValue[String]], lE: String = label): String = {
    logger.info(s"Operating on two vertices: one cached: ${vCached.vertex.id()} and one not: $lOther")
    val t0 = System.nanoTime()
    if (vCached.vertex.id().toString == idOther) throw IdNotCorrect(s"id1 $idOther should not be equal to id2")
    val vOther: VertexStructDb = new VertexStructDb(idOther, gc.g)
    if (!vOther.exist) oneExist(vCached)(vOther, pOther, lOther)(pE, lE)
    else twoExist(vCached, vOther, pE, lE)
    val t1 = System.nanoTime()
    logger.info(s"CACHED - message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "Alles gut"
  }

  /*
  If non of the two vertices that are being processed are not already present in the database.
  1/ create them.
  2/ link them.
   */
  private def noneExist(vFrom: VertexStructDb, p1: List[KeyValue[String]], l1: String)
    (vTo: VertexStructDb, p2: List[KeyValue[String]], l2: String)
    (pE: List[KeyValue[String]], lE: String): Unit = {
    logger.info("None of the two vertices were in the database")
    vFrom.addVertex(p1, l1, gc.b)
    //    verifVertex(vFrom, p1, l1)
    vTo.addVertex(p2, l2, gc.b)
    //    verifVertex(vTo, p2, l2)
    createEdge(vFrom, vTo, pE, lE)
    //    verifEdge(vFrom.id, vTo.id, pE)
  }

  /*
  If only one of the two vertices that are being processed is already present in the database.
  1/ determine which one is missing.
  2/ add it to the DB.
  3/ link them.
 */
  private def oneExist(vFrom: VertexStructDb, pFrom: List[KeyValue[String]], lFrom: String)
                      (vTo: VertexStructDb, pTo: List[KeyValue[String]], lTo: String)
                      (pE: List[KeyValue[String]], lE: String): Unit = {
    if (vFrom.exist) {
      logger.info(s"A vertex was already in the database: $lFrom")
      vTo.addVertex(pTo, lTo, gc.b)
      //      verifVertex(vTo, p2, l2)
      createEdge(vFrom, vTo, pE, lE)
      //      verifEdge(vFrom.id, vTo.id, pE)
    } else {
      logger.info(s"A vertex was already in the database: $lTo")
      vFrom.addVertex(pFrom, lFrom, gc.b)
      //      verifVertex(vFrom, p1, l1)
      createEdge(vFrom, vTo, pE, lE)
      //      verifEdge(vFrom.id, vTo.id, pE)
    }
  }

  // cached version
  private def oneExist(vCached: VertexStructDb)
                      (vTo: VertexStructDb, pTo: List[KeyValue[String]], lTo: String)
                      (pE: List[KeyValue[String]], lE: String): Unit = {
    logger.info(s"A vertex was already in the database: ${vCached.id}")
    vTo.addVertex(pTo, lTo, gc.b)
    createEdge(vCached, vTo, pE, lE)
  }

  /*
  If both vertices that are being processed is already present in the database.
  1/ link them if they're not already linked.
   */
  private def twoExist(vFrom: VertexStructDb, vTo: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    if (!areVertexLinked(vFrom, vTo)) {
      logger.info("Both vertices were already in the database")
      createEdge(vFrom, vTo, pE, lE)
      //      verifEdge(vFrom.id, vTo.id, pE)
    }
  }

  private def howMany(vFrom: VertexStructDb, vTo: VertexStructDb): Int = {
    if (vFrom.exist) {
      if (vTo.exist) 2 else 1
    } else if (vTo.exist) 1 else 0
  }

  /**
    * Create an edge between two vertices.
    *
    * @param vFrom First vertex.
    * @param vTo   Second vertex.
    * @param pE    properties of the edge that will link them.
    * @param lE    label of the edge that will link them.
    */
  private def createEdge(vFrom: VertexStructDb, vTo: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    val t0 = System.nanoTime()
    if (pE.isEmpty) {
      gc.g.V(vFrom.vertex).as("a").V(vTo.vertex).addE(lE).from(vFrom.vertex).toSet().head
    } else {
      val edge = gc.g.V(vFrom.vertex).as("a").V(vTo.vertex).addE(lE).property(pE.head).from(vFrom.vertex).toSet().head
      for (keyV <- pE.tail) {
        gc.g.E(edge).property(keyV).iterate()
      }
    }

    logger.info(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to link vertices")
  }

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  private def areVertexLinked(vFrom: VertexStructDb, vTo: VertexStructDb): Boolean = {
    val t0 = System.nanoTime()
    val res = gc.g.V(vFrom.vertex).bothE().bothV().is(vTo.vertex).l()
    //    val oneWay = gc.g.V(vFrom.vertex).outE().as("e").inV.has(ID, vTo.id).select("e").toList
    //    val otherWay = gc.g.V(vTo.vertex).outE().as("e").inV.has(ID, vFrom.id).select("e").toList
    logger.info(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to check if vertices were linked. Result: ${res.nonEmpty}")
    res.nonEmpty
  }

  /**
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
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

  /**
    * Verify if an edge has been correctly added to the janusgraph server.
    *
    * @param idFrom     Id of the vertex from where the edge originate.
    * @param idTo       Id of the vertex to where the edge goes.
    * @param properties properties of the edge.
    */
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
