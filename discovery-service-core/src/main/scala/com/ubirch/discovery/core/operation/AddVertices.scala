package com.ubirch.discovery.core.operation

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.{ImportToGremlinException, KeyNotInList, PropertiesNotCorrect}
import com.ubirch.discovery.core.util.Timer
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
  def createRelation(relation: Relation)(implicit propSet: Set[Property]): String = {
    stopIfVerticesAreEquals(relation.vFrom, relation.vTo)
    val vFrom: VertexStructDb = relation.vFrom.toVertexStructDb(gc.g)
    val vTo: VertexStructDb = relation.vTo.toVertexStructDb(gc.g)
    val relationWithVerticesInDb = RelationDb(vFrom, vTo, relation.edge)
    val timer = new Timer()
    howMany(vFrom, vTo) match {
      case 0 => noneExist(relationWithVerticesInDb)
      case 1 => oneExist(relationWithVerticesInDb)
      case 2 => twoExist(relationWithVerticesInDb)
    }
    timer.finish("add two vertex")
    "OK BB" //TODO: change this return line
  }

  private def howMany(vFrom: VertexStructDb, vTo: VertexStructDb): Int = {
    if (vFrom.existInJanusGraph) {
      if (vTo.existInJanusGraph) 2 else 1
    } else if (vTo.existInJanusGraph) 1 else 0
  }

  /*
  If non of the two vertices that are being processed are not already present in the database.
  1/ create them.
  2/ link them.
   */
  private def noneExist(relation: RelationDb): Unit = {
    try {
      relation.vFromDb.addVertexWithProperties(gc.b)
      relation.vToDb.addVertexWithProperties(gc.b)
      createEdge(relation)
    } catch {
      case e: ImportToGremlinException =>
        deleteVertices(List(relation.vFromDb, relation.vToDb))
        throw e
    }
  }

  /*
  If only one of the two vertices that are being processed is already present in the database.
  1/ determine which one is missing.
  2/ add it to the DB.
  3/ link them.
 */
  private def oneExist(relation: RelationDb): Unit = {

    def addOneVertexAndLink(v1: VertexStructDb, v2: VertexStructDb): Unit = {
      try {
        v1.addVertexWithProperties(gc.b)
        createEdge(relation)
      } catch {
        case e: ImportToGremlinException =>
          deleteVertices(List(v1))
          throw e
      }
    }

    if (relation.vFromDb.existInJanusGraph) {
      addOneVertexAndLink(relation.vToDb, relation.vFromDb)
    } else {
      addOneVertexAndLink(relation.vFromDb, relation.vToDb)
    }
  }

  def addTwoVerticesCached(vCached: VertexStructDb)(internalVertexTo: VertexToAdd)(edge: EdgeToAdd)
    (implicit propSet: Set[Property]): String = {
    logger.debug(s"Operating on two vertices: one cached: ${vCached.vertex.id()} and one not: ${internalVertexTo.label}")
    val timer = new Timer()
    stopIfVerticesAreEquals(vCached.internalVertex, internalVertexTo)
    val vTo: VertexStructDb = new VertexStructDb(internalVertexTo, gc.g)
    val relation = RelationDb(vCached, vTo, edge)
    if (!vTo.existInJanusGraph) {
      oneExistCache(relation)
    } else {
      twoExist(relation)
    }
    timer.finish("add two vertex with one CACHED")
    "Alles gut"
  }

  /**
    * vFrom is the cached vertex
    */
  private def oneExistCache(relation: RelationDb): Unit = {
    logger.debug(s"A vertex was already in the database: ${relation.vFromDb.internalVertex.properties.mkString(", ")}")
    try {
      relation.vToDb.addVertexWithProperties(gc.b)
      createEdge(relation)
    } catch {
      case e: ImportToGremlinException =>
        deleteVertices(List(relation.vToDb))
        throw e
    }
  }

  /*
  If both vertices that are being processed is already present in the database.
  1/ link them if they're not already linked.
   */
  private def twoExist(relation: RelationDb): Unit = {
    if (!areVertexLinked(relation.vFromDb, relation.vToDb)) {
      logger.debug("Both vertices were already in the database")
      createEdge(relation)
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
    val timer = new Timer()
    val res = gc.g.V(vFrom.vertex).bothE().bothV().is(vTo.vertex).l()
    timer.finish(s"check if vertices ${vFrom.vertex.id} and ${vTo.vertex.id} were linked. Result: ${res.nonEmpty}")
    res.nonEmpty
  }

  /**
    * Create an edge between two vertices.
    */
  def createEdge(relation: RelationDb): Unit = {
    val timer = new Timer
    if (relation.edge.properties.isEmpty) {
      gc.g.V(relation.vFromDb.vertex).as("a").V(relation.vToDb.vertex).addE(relation.edge.label).from(relation.vFromDb.vertex).toSet().head
    } else {
      val edge = gc.g.V(relation.vFromDb.vertex).as("a").V(relation.vToDb.vertex).addE(relation.edge.label).property(relation.edge.properties.head).from(relation.vFromDb.vertex).toSet().head
      for (keyV <- relation.edge.properties.tail) {
        gc.g.E(edge).property(keyV).iterate()
      }
    }
    timer.finish(s"link vertices of vertices ${relation.vFromDb.vertex.id} and ${relation.vToDb.vertex.id}, len(properties) = ${relation.edge.properties.size} .")
  }

  /**
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
  def verifVertex(vertexStruct: VertexStructDb, properties: List[KeyValue[String]], l: String = label): Unit = {
    if (!vertexStruct.existInJanusGraph) throw new ImportToGremlinException("Vertex wasn't imported to the Gremlin Server")

    val keyList: Array[Key[String]] = properties.map(x => x.key).toArray
    val propertiesInServer = vertexStruct.getPropertiesMap
    val propertiesInServerAsListKV = try {
      recompose(propertiesInServer, keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServerAsListKV.sortBy(x => x.key.name) == properties.sortBy(x => x.key.name))) {
      logger.debug(s"properties = ${properties.mkString(", ")}")
      logger.debug(s"propertiesInServer = ${propertiesInServerAsListKV.mkString(", ")}")
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

  def stopIfVerticesAreEquals(vertex1: VertexToAdd, vertex2: VertexToAdd): Unit = {
    if (vertex1 equals vertex2) {
      throw PropertiesNotCorrect(s"p1 = ${vertex1.properties.map(x => s"${x.key.name} = ${x.value}, ")} should not be equal to the properties of the second vertex")
    }
  }
}
