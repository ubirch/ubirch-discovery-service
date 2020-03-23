package com.ubirch.discovery.core.operation

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.{Timer, Util}
import com.ubirch.discovery.core.util.Exceptions.{ImportToGremlinException, KeyNotInList, PropertiesNotCorrect}
import com.ubirch.discovery.core.util.Util.{getEdge, getEdgeProperties, recompose}
import com.ubirch.janusgraph.GremlinConnector

import scala.language.postfixOps
import scala.util.Success

/**
  * Allows the storage of two nodes (vertices) in the janusgraph server. Link them together
  *
  * @param gc A GremlinConnector connected to a janusgraph server
  */
case class AddRelation()(implicit gc: GremlinConnector) extends LazyLogging {

  private val label = "aLabel"

  /* main part of the program */
  def createRelation(relation: Relation)(implicit propSet: Set[Property]): String = {
    Timer.time({
      stopIfVerticesAreEquals(relation.vFrom, relation.vTo)
      val relationServer = relation.toRelationServer
      executeRelationCreationStrategy(relationServer)
    }, "add two vertices").logTimeTaken()
    "OK BB" //TODO: change this return line
  }

  def executeRelationCreationStrategy(relationServer: RelationServer): Unit = {
    howManyVerticesAlreadyInDb(List(relationServer.vFromDb, relationServer.vToDb)) match {
      case 0 => noneExist(relationServer)
      case 1 => oneExist(relationServer)
      case 2 => twoExist(relationServer)
    }
  }

  private def howManyVerticesAlreadyInDb(vertices: List[VertexDatabase]): Int = vertices.count(v => v.existInJanusGraph)

  /*
  If non of the two vertices that are being processed are not already present in the database.
  1/ create them.
  2/ link them.
   */
  private def noneExist(relation: RelationServer): Unit = {
    logger.debug(Util.relationStrategyJson(relation, "non exist"))
    try {
      relation.vFromDb.addVertexWithProperties()
      relation.vToDb.addVertexWithProperties()
      relation.createEdge
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
  private def oneExist(relation: RelationServer): Unit = {
    def addOneVertexAndCreateEdge(vertexNotInDb: VertexDatabase): Unit = {
      try {
        vertexNotInDb.addVertexWithProperties()
        relation.createEdge
      } catch {
        case e: ImportToGremlinException =>
          deleteVertices(List(vertexNotInDb))
          throw e
      }
    }

    if (relation.vFromDb.existInJanusGraph) {
      logger.debug(Util.relationStrategyJson(relation, "one exit: vFrom"))
      addOneVertexAndCreateEdge(relation.vToDb)
    } else {
      logger.debug(Util.relationStrategyJson(relation, "one exit: vTo"))
      addOneVertexAndCreateEdge(relation.vFromDb)
    }
  }

  def addTwoVerticesCached(vCached: VertexDatabase)(internalVertexTo: VertexCore)(edge: EdgeCore)
    (implicit propSet: Set[Property]): String = {
    Timer.time({
      stopIfVerticesAreEquals(vCached.coreVertex, internalVertexTo)
      val vTo: VertexDatabase = internalVertexTo.toVertexStructDb(gc)
      val relation = RelationServer(vCached, vTo, edge)
      if (!vTo.existInJanusGraph) {
        oneExistCache(relation)
      } else {
        twoExist(relation)
      }
    }, "add two vertex with one CACHED").logTimeTaken()
    "Alles gut"
  }

  /**
    * vFrom is the cached vertex
    */
  private def oneExistCache(relation: RelationServer): Unit = {
    logger.debug(Util.relationStrategyJson(relation, "one exit cached: vFrom"))
    try {
      relation.vToDb.addVertexWithProperties()
      relation.createEdge
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
  private def twoExist(relation: RelationServer): Unit = {
    logger.debug(Util.relationStrategyJson(relation, "two exist"))

    if (!areVertexLinked(relation.vFromDb, relation.vToDb)) {
      relation.createEdge
    }
  }

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  def areVertexLinked(vFrom: VertexDatabase, vTo: VertexDatabase): Boolean = {
    val timedResult = Timer.time(gc.g.V(vFrom.vertex).bothE().bothV().is(vTo.vertex).l())
    timedResult.result match {
      case Success(value) =>
        timedResult.logTimeTaken(s"check if vertices ${vFrom.vertex.id} and ${vTo.vertex.id} were linked. Result: ${value.nonEmpty}")
        value.nonEmpty
    }
  }

  /**
    * Verify if a vertex has been correctly added to the janusgraph server.
    *
    * @param vertexStruct a VertexStruct representing the vertex.
    * @param properties   properties of the vertex that should have been added correctly.
    * @param l            label of the vertex.
    */
  def verifVertex(vertexStruct: VertexDatabase, properties: List[ElementProperty], l: String = label): Unit = {
    if (!vertexStruct.existInJanusGraph) throw new ImportToGremlinException("Vertex wasn't imported to the Gremlin Server")

    val keyList = properties.map(x => x.keyName)
    val propertiesInServer = vertexStruct.getPropertiesMap
    val propertiesInServerAsListKV = try {
      recompose(propertiesInServer, keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
      case x: Throwable => throw x
    }
    if (!(propertiesInServerAsListKV.sortBy(p => p.keyName) == properties.sortBy(p => p.keyName))) {
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
  def verifEdge(vFrom: VertexDatabase, vTo: VertexDatabase, properties: List[ElementProperty]): Unit = {
    val edge = getEdge(gc, vFrom, vTo).head

    if (edge == null) throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't created")

    val keyList = properties map (x => x.keyName)
    val propertiesInServer = try {
      recompose(getEdgeProperties(gc, edge), keyList)
    } catch {
      case _: KeyNotInList => throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
      case x: Throwable => throw x
    }

    if (!(propertiesInServer.sortBy(x => x.keyName) == properties.sortBy(x => x.keyName)))
      throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
  }

  private def deleteVertices(verticesToDelete: List[VertexDatabase]): Unit = {
    logger.warn(s"Deleting vertices: ${verticesToDelete.map(v => "label: " + v.vertex.label() + " id: " + v.vertexId).mkString(", ")}")
    verticesToDelete foreach { v => v.deleteVertex() }
  }

  def stopIfVerticesAreEquals(vertex1: VertexCore, vertex2: VertexCore): Unit = {
    if (vertex1 equals vertex2) {
      throw PropertiesNotCorrect(s"p1 = ${vertex1.properties.map(x => s"${x.keyName} = ${x.value}, ")} should not be equal to the properties of the second vertex")
    }
  }
}
