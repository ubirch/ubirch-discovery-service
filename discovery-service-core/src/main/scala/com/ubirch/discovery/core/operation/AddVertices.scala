package com.ubirch.discovery.core.operation

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.{ImportToGremlinException, KeyNotInList, PropertiesNotCorrect}
import com.ubirch.discovery.core.util.Util.{getEdge, getEdgeProperties, recompose}
import gremlin.scala.{Key, KeyValue}

import scala.language.postfixOps

/**
  * Allows the storage of two nodes (vertices) in the janusgraph server. Link them together
  *
  * @param gc A GremlinConnector connected to a janusgraph server
  */
case class AddVertices()(implicit gc: GremlinConnector) extends LazyLogging {

  private val label = "aLabel"

  /* main part of the program */
  def addTwoVertices(p1: List[KeyValue[String]], l1: String)
                    (p2: List[KeyValue[String]], l2: String)
                    (pE: List[KeyValue[String]], lE: String): String = {

    if (p1.sortBy(x => x.key.name) equals p2.sortBy(x => x.key.name)) throw PropertiesNotCorrect(s"p1 = ${p1.map(x => s"${x.key.name} = ${x.value}, ")} should not be equal to the properties of the second vertex")
    val vFrom: VertexStructDb = new VertexStructDb(p1, gc.g, l1)
    val vTo: VertexStructDb = new VertexStructDb(p2, gc.g, l2)
    val t0 = System.nanoTime()
    howMany(vFrom, vTo) match {
      case 0 => noneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 1 => oneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 2 => twoExist(vFrom, vTo, pE, lE)
    }
    val t1 = System.nanoTime()
    logger.info(s"message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "OK BB" //TODO: change this return line
  }

  private def howMany(vFrom: VertexStructDb, vTo: VertexStructDb): Int = {
    if (vFrom.exist) {
      if (vTo.exist) 2 else 1
    } else if (vTo.exist) 1 else 0
  }

  /*
  If non of the two vertices that are being processed are not already present in the database.
  1/ create them.
  2/ link them.
   */
  private def noneExist(vFrom: VertexStructDb, p1: List[KeyValue[String]], l1: String)
                       (vTo: VertexStructDb, p2: List[KeyValue[String]], l2: String)
                       (pE: List[KeyValue[String]], lE: String): Unit = {
    try {
      vFrom.addVertex(p1, l1, gc.b)
      vTo.addVertex(p2, l2, gc.b)
      createEdge(vFrom, vTo, pE, lE)
    } catch {
      case e: ImportToGremlinException =>
        deleteVertices(List(vFrom, vTo))
        throw e
    }
  }

  /*
  If only one of the two vertices that are being processed is already present in the database.
  1/ determine which one is missing.
  2/ add it to the DB.
  3/ link them.
 */
  private def oneExist(vFrom: VertexStructDb, p1: List[KeyValue[String]], l1: String)
                      (vTo: VertexStructDb, p2: List[KeyValue[String]], l2: String)
                      (pE: List[KeyValue[String]], lE: String): Unit = {
    if (vFrom.exist) {
      try {
        vTo.addVertex(p2, l2, gc.b)
        createEdge(vFrom, vTo, pE, lE)
      } catch {
        case e: ImportToGremlinException =>
          deleteVertices(List(vTo))
          throw e
      }
    } else {
      try {
        vFrom.addVertex(p1, l1, gc.b)
        createEdge(vFrom, vTo, pE, lE)
      } catch {
        case e: ImportToGremlinException =>
          deleteVertices(List(vFrom))
          throw e
      }
    }
  }

  def addTwoVerticesCached(vCached: VertexStructDb)
                          (pOther: List[KeyValue[String]], lOther: String = label)
                          (pE: List[KeyValue[String]], lE: String = label): String = {
    logger.info(s"Operating on two vertices: one cached: ${vCached.vertex.id()} and one not: $lOther")
    val t0 = System.nanoTime()
    if (vCached.properties.sortBy(x => x.key.name) == pOther.sortBy(x => x.key.name)) throw PropertiesNotCorrect(s"v1 should not be equal to v2")
    val vOther: VertexStructDb = new VertexStructDb(pOther, gc.g, lOther)
    if (!vOther.exist) oneExistCache(vCached)(vOther, pOther, lOther)(pE, lE)
    else twoExist(vCached, vOther, pE, lE)
    val t1 = System.nanoTime()
    logger.info(s"CACHED - message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "Alles gut"
  }


  private def oneExistCache(vCached: VertexStructDb)
                           (vTo: VertexStructDb, pTo: List[KeyValue[String]], lTo: String)
                           (pE: List[KeyValue[String]], lE: String): Unit = {
    logger.info(s"A vertex was already in the database: ${vCached.properties.mkString(", ")}")
    try {
      vTo.addVertex(pTo, lTo, gc.b)
      createEdge(vCached, vTo, pE, lE)
    } catch {
      case e: ImportToGremlinException =>
        deleteVertices(List(vTo))
        throw e
    }
  }

  /*
  If both vertices that are being processed is already present in the database.
  1/ link them if they're not already linked.
   */
  private def twoExist(vFrom: VertexStructDb, vTo: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    if (!areVertexLinked(vFrom, vTo)) {
      logger.info("Both vertices were already in the database")
      createEdge(vFrom, vTo, pE, lE)
    }
  }

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  def areVertexLinked(vFrom: VertexStructDb, vTo: VertexStructDb): Boolean = {
    val t0 = System.nanoTime()
    val res = gc.g.V(vFrom.vertex).bothE().bothV().is(vTo.vertex).l()
    logger.info(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to check if vertices were linked. Result: ${res.nonEmpty}")
    res.nonEmpty
  }

  /**
    * Create an edge between two vertices.
    *
    * @param vFrom First vertex.
    * @param vTo   Second vertex.
    * @param pE    properties of the edge that will link them.
    * @param lE    label of the edge that will link them.
    */
  def createEdge(vFrom: VertexStructDb, vTo: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
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
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
  def verifVertex(vertexStruct: VertexStructDb, properties: List[KeyValue[String]], l: String = label): Unit = {
    if (!vertexStruct.exist) throw new ImportToGremlinException("Vertex wasn't imported to the Gremlin Server")

    val keyList: Array[Key[String]] = properties.map(x => x.key).toArray
    val propertiesInServer = vertexStruct.getPropertiesMap
    val propertiesInServerAsListKV = try {
      recompose(propertiesInServer, keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServerAsListKV.sortBy(x => x.key.name) == properties.sortBy(x => x.key.name))) {
      logger.info(s"properties = ${properties.mkString(", ")}")
      logger.info(s"propertiesInServer = ${propertiesInServerAsListKV.mkString(", ")}")
      throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
    }
  }

  /**
    * Verify if an edge has been correctly added to the janusgraph server.
    *
    * @param vFrom      Id of the vertex from where the edge originate.
    * @param vTo        Id of the vertex to where the edge goes.
    * @param properties properties of the edge.
    */
  def verifEdge(vFrom: VertexStructDb, vTo: VertexStructDb, properties: List[KeyValue[String]]): Unit = {
    val edge = getEdge(gc, vFrom, vTo).head

    if (edge == null) throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't created")

    val keyList = properties map (x => x.key) toArray
    val propertiesInServer = try {
      recompose(getEdgeProperties(gc, edge), keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
      case x: Throwable => throw x
    }

    if (!(propertiesInServer.sortBy(x => x.key.name) == properties.sortBy(x => x.key.name)))
      throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
  }

  private def deleteVertices(verticesToDelete: List[VertexStructDb]): Unit = {
    verticesToDelete foreach { v => v.deleteVertex() }
  }
}
