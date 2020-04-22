package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddRelation.{ areVertexLinked, logger }
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.{ GraphException, ImportToGremlinException, VertexCreationException, VertexUpdateException }
import com.ubirch.discovery.core.util.Util
import gremlin.scala.{ Edge, Vertex }
import org.janusgraph.core.SchemaViolationException

import scala.annotation.tailrec
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.collection.JavaConverters._
import scala.collection.immutable

object Helpers extends LazyLogging {

  /**
    * Determine if two vertices are linked (independently of the direction of the edge).
    *
    * @param vFrom first vertex.
    * @param vTo   second vertex.
    * @return boolean. True = linked, False = not linked.
    */
  def areVertexLinked(vFrom: Vertex, vTo: Vertex)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Boolean] = {
    gc.g.V(vFrom).both().is(vTo).promise().map(_.nonEmpty)
  }

  def createRelation(vFrom: Vertex, vTo: Vertex, edge: EdgeCore)(implicit ec: ExecutionContext, gc: GremlinConnector): Future[Option[Edge]] = {

    def recoverEdge(error: Throwable): Future[Option[Edge]] = {
      areVertexLinked(vFrom, vTo).flatMap { linked =>
        if (!linked) for {
          edges <- createEdgeTraversalPromise(vFrom, vTo, edge)
        } yield {
          edges.headOption
        }
        else Util.getOneEdge(vFrom, vTo)
      }
    }

    val a: Future[Option[Edge]] = for {
      createdEdge: Option[Edge] <- for {
        edges <- createEdgeTraversalPromise(vFrom, vTo, edge)
      } yield {
        edges.headOption
      }
    } yield {
      createdEdge
    }

    a.recoverWith {
      case e: CompletionException => recoverEdge(e)
      case e: SchemaViolationException => recoverEdge(e)
    }.recoverWith {
      case e: Exception =>
        logger.error("error initialising vertex", e)
        Future.failed(e)
    }

  }

  private def createEdgeTraversalPromise(vFrom: Vertex, vTo: Vertex, edge: EdgeCore)(implicit gc: GremlinConnector): Future[List[Edge]] = {
    var constructor = gc.g.V(vTo).addE(edge.label)
    for (prop <- edge.properties) {
      constructor = constructor.property(prop.toKeyValue)
    }
    constructor.from(vFrom).promise()
  }

  def getOrCreateVertex(vertexCore: VertexCore)(implicit ec: ExecutionContext, gc: GremlinConnector, schemaDefinedProperties: Set[Property]): Option[Vertex] = {
    import scala.concurrent.duration._

    val r = for {
      maybeVertex: Option[Vertex] <- searchForVertexByProperties(vertexCore.properties)
    } yield {
      maybeVertex match {
        case Some(concreteVertex) =>
          logger.debug(s"Found vertex ${vertexCore.toString}, updating it")
          update(concreteVertex, vertexCore)
          Future.successful(Some(concreteVertex))
        case None =>
          logger.debug(s"  val vertex: Future[Option[Vertex]] = { - didn't find vertex ${vertexCore.toString}")
          addVertexWithProperties(vertexCore)
      }
    }
    Await.result(r.flatten, 1.second)
  }

  /**
    * Adds a vertex in the database with his label and properties.
    */
  def addVertexWithProperties(vertexCore: VertexCore)(implicit gc: GremlinConnector, ec: ExecutionContext, schemaDefinedProperties: Set[Property]): Future[Option[Vertex]] = {
    logger.debug(s"adding vertex ${vertexCore.toString}")

    initialiseVertex(vertexCore) recover {
      case e: CompletionException =>
        logger.error(s"Error on adding vertex and its properties ${vertexCore.toString}: ", e)
        throw new ImportToGremlinException(s"Error on adding properties to vertex ${vertexCore.toString}: " + e.getMessage) //TODO: do something
      case e: Exception =>
        logger.error(s"Error on adding vertex and its properties ${vertexCore.toString}: ", e)
        throw VertexCreationException(s"Error on adding vertex and its properties ${vertexCore.toString}: ", e)
    }
  }

  private def initialiseVertex(vertexCore: VertexCore)(implicit gc: GremlinConnector, ec: ExecutionContext, schemaDefinedProperties: Set[Property]): Future[Option[Vertex]] = {

    buildAddVertex(vertexCore)
      .map(x => x.headOption)
      .recoverWith {
        case e: CompletionException => recoverVertexAlreadyExist(vertexCore, e)
        case e: SchemaViolationException => recoverVertexAlreadyExist(vertexCore, e)
        case e: Exception =>
          logger.error("error initialising vertex", e)
          Future.failed(VertexCreationException(s"error initialising vertex ${vertexCore.toString}", e))
      }

  }

  def buildAddVertex(vertexCore: VertexCore)(implicit gc: GremlinConnector): Future[List[Vertex]] = {
    var constructor = gc.g.addV(vertexCore.label)
    for (prop <- vertexCore.properties) {
      constructor = constructor.property(prop.toKeyValue)
    }
    constructor.promise()
  }

  /**
    * When adding vertices asynchronously, a vertex or a vertex property can have already been added by another thread or instance
    * Then it's not an actual error that we're catching
    */
  private def recoverVertexAlreadyExist(vertexCore: VertexCore, error: Throwable)(implicit gc: GremlinConnector, ec: ExecutionContext, schemaDefinedProperties: Set[Property]): Future[Option[Vertex]] = {
    logger.warn("uniqueness constraint, recovrecoverVertexAlreadyExistering" + error.getMessage)

    searchForVertexByProperties(vertexCore.properties)
      .flatMap {
        case actualVertex @ Some(_) => Future.successful(actualVertex)
        case None => buildAddVertex(vertexCore).map(_.headOption)
      }

  }

  /**
    * Add new properties to vertex
    */
  def update(vertexToUpdate: Vertex, vertexShouldBe: VertexCore)(implicit ec: ExecutionContext, gc: GremlinConnector): Future[Option[Vertex]] = {
    val shouldBeProps: Map[String, String] = vertexShouldBe.properties.map { ep => ep.keyName -> ep.value.toString }.toMap
    val r = for {
      map: Map[String, String] <- getPropertiesMap(vertexToUpdate).map(_.map { kv => kv._1.toString -> kv._2.head.toString })
    } yield {
      if (map.contains("timestamp")) {
        areTheSame(vertexToUpdate, map, vertexShouldBe)

      } else if (shouldBeProps.contains("timestamp")) {
        addNewPropertiesToVertex(vertexToUpdate, vertexShouldBe)

      } else {
        Future(Option(vertexToUpdate))
      }
    }
    r.flatMap(identity)

  }

  // check if the properties of the vertex already in janus and the "new" ones are the same
  // if not, add them
  // not checking the timestamp as it's always a pain in the ass to cast
  private def areTheSame(vertex: Vertex, currentVertexMap: Map[String, String], vertexShouldBe: VertexCore)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Option[Vertex]] = {
    val shouldBeProps: Map[String, String] = vertexShouldBe.properties.map { ep => ep.keyName -> ep.value.toString }.toMap
    if (!areMapsTheSame(currentVertexMap, shouldBeProps)) addNewPropertiesToVertex(vertex, vertexShouldBe)
    else Future.successful(None)
  }

  private def areMapsTheSame(mapOne: Map[String, String], mapTwo: Map[String, String]): Boolean = {
    mapOne - "timestamp" == mapTwo - "timestamp"
  }

  private def addNewPropertiesToVertex(vertex: Vertex, vertexCore: VertexCore)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Option[Vertex]] = {

    /**
      * Will build a gremlin constructor
      * @return
      */
    def buildAddProperty = {
      var constructor = gc.g.V(vertex)
      for (prop <- vertexCore.properties) {
        constructor = constructor.property(prop.toKeyValue)
      }
      constructor
    }

    for {
      constructor <- Future(buildAddProperty)
      res <- constructor.promise()
        .map(x => x.headOption)
        .recover {
          case e: Exception =>
            throw VertexUpdateException(s"Error adding properties on vertex ${vertex.toString}", e)
        }
    } yield res

  }

  /**
    * Returns a Map<Any, List<Any>> fo the properties. A vertex property can have a list of values, thus why
    * the method is returning this kind of structure.
    *
    * @return A map containing the properties name and respective values of the vertex contained in this structure.
    */
  def getPropertiesMap(vertex: Vertex)(implicit gc: GremlinConnector, ec: ExecutionContext): Future[Map[Any, List[Any]]] = {
    logger.debug("looking for property map")

    logger.debug(s"found vertex: }")
    for {
      res <- gc.g.V(vertex).valueMap.promise()
    } yield {
      logger.debug("found valueMap")
      val propertyMapAsJava = res.headOption match {
        case Some(v) => v.asScala.toMap.asInstanceOf[Map[Any, util.ArrayList[Any]]]
        case None => throw new Exception(s"cannot find valueMap of vertex ${vertex.id()} even though vertex exist")
      }

      propertyMapAsJava map { x => x._1 -> x._2.asScala.toList }

    }

  }

  /**
    * Look if a vertex containing at least one of the iterable properties defined in properties is present in the graph
    * This function will iterate on the @properties, select those that are iterable (ie: only one value can exist
    * for this key on the graph), then see if a vertex contains this key-value in the graph.
    * If one is found, it'll return it
    * Else, it'll return null
    * @param properties the list of the vertex properties
    * @return if found, the vertex who has at least one of those iterable properties
    */
  private def searchForVertexByProperties(properties: List[ElementProperty])(implicit ec: ExecutionContext, gc: GremlinConnector, schemaDefinedProperties: Set[Property]): Future[Option[gremlin.scala.Vertex]] = {
    properties match {
      case Nil => Future.successful(None)
      case property :: restOfProperties if !isPropertyIterable(property.keyName) =>
        searchForVertexByProperties(restOfProperties)
      case property :: restOfProperties =>
        // if more than one an error should thrown, as the graph would be invalid
        gc.g.V().has(property.toKeyValue).promise().flatMap {
          case Nil => searchForVertexByProperties(restOfProperties) // if nothing is found then continue to look for another property
          case List(x) => Future.successful(Option(x)) // will it not tho
          case x :: xs =>
            logger.error(s"More than one vertices having the iterable property ${property.toKeyValue.toString} have been found in the graph: ${x.id().toString}, ${xs.map(x => x.id().toString).mkString(", ")}")
            Future.failed(
              GraphException(s"More than one vertices having the iterable property ${property.toKeyValue.toString} have been found in the graph: ${x.id().toString}, ${xs.map(x => x.id().toString).mkString(", ")}")
            ) //error
        }
    }
  }

  def isPropertyIterable(propertyName: String)(implicit schemaDefinedProperties: Set[Property]): Boolean = {
    @tailrec
    def checkOnProps(set: Set[Property]): Boolean = {
      set.toList match {
        case Nil => false
        case x => if (x.head.name == propertyName) {
          if (x.head.isPropertyUnique) true else checkOnProps(x.tail.toSet)
        } else {
          checkOnProps(x.tail.toSet)
        }
      }
    }
    checkOnProps(schemaDefinedProperties)

  }

}
