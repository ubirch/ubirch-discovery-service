package com.ubirch.discovery.core.operation

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.{IdNotCorrect, ImportToGremlinException, KeyNotInList}
import com.ubirch.discovery.core.util.Util.{extractValue, getEdge, getEdgeProperties, recompose}
import gremlin.scala.{Key, KeyValue}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

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
    logger.debug(s"operating on two vertices: one $l1 and one $l2")
    val t0 = System.nanoTime()
    if (id1 == id2) throw IdNotCorrect(s"id1 $id1 should not be equal to id2")
    val (vFrom, vTo) = getVFromVTo(id1, id2)
    howMany(vFrom, vTo) match {
      case 0 => noneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 1 => oneExist(vFrom, p1, l1)(vTo, p2, l2)(pE, lE)
      case 2 => twoExist(vFrom, vTo, pE, lE)
    }
    val t1 = System.nanoTime()
    logger.debug(s"INTERNAL - message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "OK BB" //TODO: change this return line
  }

  /*
  If non of the two vertices that are being processed are not already present in the database.
  1/ create them.
  2/ link them.
   */
  private def noneExist(vFrom: VertexStructDb, p1: List[KeyValue[String]], l1: String)
    (vTo: VertexStructDb, p2: List[KeyValue[String]], l2: String)
    (pE: List[KeyValue[String]], lE: String): Unit = {
    logger.debug("None of the two vertices were in the database")
    addVertexAsync(List((vFrom, p1, l1), (vTo, p2, l2)))
    /*    vFrom.addVertex(p1, l1, gc.b)
        vTo.addVertex(p2, l2, gc.b)*/
    createEdge(vFrom, vTo, pE, lE)
  }

  def addVertexAsync(vertices: List[(VertexStructDb, List[KeyValue[String]], String)]): Unit = {
    val processesOfFutures: ListBuffer[Future[Unit]] = scala.collection.mutable.ListBuffer.empty[Future[Unit]]
    vertices.foreach { v =>
      val process = Future(v._1.addVertex(v._2, v._3, gc.b))
      processesOfFutures += process
    }

    val futureProcesses: Future[ListBuffer[Unit]] = Future.sequence(processesOfFutures)

    val latch = new CountDownLatch(1)

    futureProcesses.onComplete {
      case Success(l) =>
        latch.countDown()
      case Failure(e) =>
        throw e
        latch.countDown()
    }
    latch.await()
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
      logger.debug(s"A vertex was already in the database: $lFrom")
      vTo.addVertex(pTo, lTo, gc.b)
      createEdge(vFrom, vTo, pE, lE)
    } else {
      logger.debug(s"A vertex was already in the database: $lTo")
      vFrom.addVertex(pFrom, lFrom, gc.b)
      createEdge(vFrom, vTo, pE, lE)
    }
  }

  def addTwoVerticesCached(vCached: VertexStructDb)
                          (idOther: String, pOther: List[KeyValue[String]], lOther: String = label)
                          (pE: List[KeyValue[String]], lE: String = label): String = {
    logger.debug(s"Operating on two vertices: one cached: ${vCached.vertex.id()} and one not: $lOther")
    val t0 = System.nanoTime()
    if (vCached.vertex.id().toString == idOther) throw IdNotCorrect(s"id1 $idOther should not be equal to id2")
    val vOther: VertexStructDb = new VertexStructDb(idOther, gc.g)
    if (!vOther.exist) oneExist(vCached)(vOther, pOther, lOther)(pE, lE)
    else twoExist(vCached, vOther, pE, lE)
    val t1 = System.nanoTime()
    logger.debug(s"CACHED - message processed in ${(t1 / 1000000 - t0 / 1000000).toString} ms")
    "Alles gut"
  }

  def getVFromVTo(idVFrom: String, idVTo: String): (VertexStructDb, VertexStructDb) = {
    val res = createVertexStructDbAsync(List(idVFrom, idVTo))
    (res.head, res(1))
  }

  /*
  If both vertices that are being processed is already present in the database.
  1/ link them if they're not already linked.
   */
  private def twoExist(vFrom: VertexStructDb, vTo: VertexStructDb, pE: List[KeyValue[String]], lE: String): Unit = {
    if (!areVertexLinked(vFrom, vTo)) {
      logger.debug("Both vertices were already in the database")
      createEdge(vFrom, vTo, pE, lE)
    }
  }

  private def howMany(vFrom: VertexStructDb, vTo: VertexStructDb): Int = {
    if (vFrom.exist) {
      if (vTo.exist) 2 else 1
    } else if (vTo.exist) 1 else 0
  }

  def createVertexStructDbAsync(ids: List[String]): List[VertexStructDb] = {
    val processesOfFutures: ListBuffer[Future[VertexStructDb]] = scala.collection.mutable.ListBuffer.empty[Future[VertexStructDb]]
    ids.foreach { id =>
      val process = Future(new VertexStructDb(id, gc.g))
      processesOfFutures += process
    }

    val futureProcesses: Future[ListBuffer[VertexStructDb]] = Future.sequence(processesOfFutures)

    val latch = new CountDownLatch(1)

    futureProcesses.onComplete {
      case Success(l) =>
        latch.countDown()
        l
      case Failure(e) =>
        throw e
        latch.countDown()
        scala.collection.mutable.ListBuffer.empty[Future[VertexStructDb]]
    }
    latch.await()
    Await.result(futureProcesses, 1 second).toList
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
    val res = gc.g.V(vTo.vertex).bothE().bothV().is(vFrom.vertex).l()
    logger.debug(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to check if vertices ${vFrom.vertex.label()} and ${vTo.vertex.label()} were linked. Result: ${res.nonEmpty}")
    res.nonEmpty
  }

  // cached version
  private def oneExist(vCached: VertexStructDb)
                      (vTo: VertexStructDb, pTo: List[KeyValue[String]], lTo: String)
                      (pE: List[KeyValue[String]], lE: String): Unit = {
    logger.debug(s"A vertex was already in the database: ${vCached.id}")
    vTo.addVertex(pTo, lTo, gc.b)
    createEdge(vCached, vTo, pE, lE)
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

    logger.debug(s"Took ${(System.nanoTime() / 1000000 - t0 / 1000000).toString} ms to link vertices, len(properties) = ${pE.size} .")
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
