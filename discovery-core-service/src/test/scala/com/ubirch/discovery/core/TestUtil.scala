package com.ubirch.discovery.core

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.{ElementProperty, Elements, VertexDatabase}
import com.ubirch.discovery.core.util.Exceptions.{ImportToGremlinException, KeyNotInList}
import com.ubirch.discovery.core.util.Util.{getEdge, getEdgeProperties, recompose}

import scala.concurrent.{Await, ExecutionContext}

object TestUtil {

  def putPropsOnPropSet(propList: List[ElementProperty]): Set[Elements.Property] = {
    propList.map { prop => new Elements.Property(prop.keyName, true) }.toSet
  }

  def isAllDigits(x: String): Boolean = x forall Character.isDigit

  def verifVertex(vertexStruct: VertexDatabase, properties: List[ElementProperty], l: String = "aLabel")(implicit gc: GremlinConnector, ec: ExecutionContext): Unit = {

    val keyList = properties.map(x => x.keyName)
    import scala.concurrent.duration._
    for {
      v <- vertexStruct.getPropertiesMap()
    } yield {
      val propertiesInServerAsListKV = try {
        recompose(v, keyList)
      } catch {
        case _: KeyNotInList => throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
        case x: Throwable => throw x
      }
      if (!(propertiesInServerAsListKV.sortBy(p => p.keyName) == properties.sortBy(p => p.keyName))) {
        throw new ImportToGremlinException(s"Vertex with properties = ${properties.mkString(", ")} wasn't correctly imported to the database: properties are not correct")
      }
    }

  }

  /**
    * Verify if an edge has been correctly added to the janusgraph server.
    *
    * @param vFrom      Id of the vertex from where the edge originate.
    * @param vTo        Id of the vertex to where the edge goes.
    * @param properties properties of the edge.
    */
  def verifEdge(vFrom: VertexDatabase, vTo: VertexDatabase, properties: List[ElementProperty])(implicit gc: GremlinConnector, ec: ExecutionContext): Unit = {
    for {
      edge <- getEdge(gc, vFrom, vTo, 1, ec)
    } yield {

      if (edge == null) throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't created")

      val keyList = properties map (x => x.keyName)
      val propertiesInServer = try {
        recompose(getEdgeProperties(gc, edge.head), keyList)
      } catch {
        case _: KeyNotInList => throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
        case x: Throwable => throw x
      }

      if (!(propertiesInServer.sortBy(x => x.keyName) == properties.sortBy(x => x.keyName)))
        throw new ImportToGremlinException(s"Edge between $vFrom and $vTo wasn't correctly created: properties are not correct")
    }
    }


}
