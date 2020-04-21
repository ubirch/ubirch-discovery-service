package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.{ GraphException, ImportToGremlinException, VertexCreationException, VertexUpdateException }
import gremlin.scala.{ TraversalSource, Vertex }
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.janusgraph.core.SchemaViolationException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

class VertexDatabase(val coreVertex: VertexCore, val gc: GremlinConnector)(implicit schemaDefinedProperties: Set[Property], ec: ExecutionContext) extends LazyLogging {

  val g: TraversalSource = gc.g
  val b: Bindings = gc.b

  val vertex: Future[Option[Vertex]] = {
    searchForVertexByProperties(coreVertex.properties).flatMap {
      case Some(possibleVertex) => update(possibleVertex)
      case None => addVertexWithProperties()
    }
  }

  override def toString: String = coreVertex.toString

  /**
    * Adds a vertex in the database with his label and properties.
    */
  def addVertexWithProperties(): Future[Option[Vertex]] = {
    logger.debug(s"adding vertex ${coreVertex.toString}")

    initialiseVertex recover {
      case e: CompletionException =>
        logger.error(s"Error on adding vertex and its properties ${coreVertex.toString}: ", e)
        throw new ImportToGremlinException(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage) //TODO: do something
      case e: Exception =>
        logger.error(s"Error on adding vertex and its properties ${coreVertex.toString}: ", e)
        throw VertexCreationException(s"Error on adding vertex and its properties ${coreVertex.toString}: ", e)
    }
  }

  def buildAddVertex: Future[List[Vertex]] = {
    var constructor = gc.g.addV(coreVertex.label)
    for (prop <- coreVertex.properties) {
      constructor = constructor.property(prop.toKeyValue)
    }
    constructor.promise()
  }

  /**
    * Add new properties to vertex
    */
  def update(vertex: Vertex): Future[Option[Vertex]] = {
    val shouldBeProps: Map[String, String] = coreVertex.properties.map { ep => ep.keyName -> ep.value.toString }.toMap
    val r = for {
      map: Map[String, String] <- getPropertiesMap.map(_.map { kv => kv._1.toString -> kv._2.head.toString })
    } yield {
      if (map.contains("timestamp")) {
        areTheSame(vertex, map, shouldBeProps)

      } else if (shouldBeProps.contains("timestamp")) {
        addNewPropertiesToVertex(vertex)

      } else {
        Future(Option(vertex))
      }
    }
    r.flatMap(identity)

  }

  // check if the properties of the vertex already in janus and the "new" ones are the same
  // not checking the timestamp as it's always a pain in the ass to cast
  private def areTheSame(vertex: Vertex, currentVertexMap: Map[String, String], shouldBeProps: Map[String, String]): Future[Option[Vertex]] = {
    val shouldBeMap: Map[String, String] = coreVertex.properties.map { ep => ep.keyName -> ep.value.toString }.toMap
    if (!areMapsTheSame(currentVertexMap, shouldBeMap)) addNewPropertiesToVertex(vertex)
    else Future.successful(None)
  }

  private def areMapsTheSame(mapOne: Map[String, String], mapTwo: Map[String, String]): Boolean = {
    mapOne - "timestamp" == mapTwo - "timestamp"
  }

  private def addNewPropertiesToVertex(vertex: Vertex): Future[Option[Vertex]] = {

    /**
      * Will build a gremlin constructor
      * @return
      */
    def buildAddProperty = {
      var constructor = gc.g.V(vertex)
      for (prop <- coreVertex.properties) {
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
  def getPropertiesMap: Future[Map[Any, List[Any]]] = {

    val futureVertex: Future[Vertex] = for {
      maybeVertex: Option[Vertex] <- vertex
    } yield {
      maybeVertex match {
        case Some(v) => v
        case None => throw new Exception(s"cannot find vertex ${coreVertex.toString} on getPropertiesMap")
      }
    }

    for {
      vertex <- futureVertex
      res <- g.V(vertex).valueMap.promise()
    } yield {

      val propertyMapAsJava = res.headOption match {
        case Some(v) => v.asScala.asInstanceOf[Map[Any, util.ArrayList[Any]]]
        case None => throw new Exception(s"cannot find valueMap of vertex ${coreVertex.toString} even though vertex exist")
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
  private def searchForVertexByProperties(properties: List[ElementProperty])(implicit ec: ExecutionContext): Future[Option[gremlin.scala.Vertex]] = {
    properties match {
      case Nil => Future.successful(None)
      case property :: restOfProperties if !isPropertyIterable(property.keyName) =>
        searchForVertexByProperties(restOfProperties)
      case property :: restOfProperties =>
        // if more than one an error should thrown, as the graph would be invalid
        g.V().has(property.toKeyValue).promise().flatMap {
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

  private def initialiseVertex: Future[Option[Vertex]] = {

    buildAddVertex
      .map(x => x.headOption)
      .recoverWith {
        case e: CompletionException => recoverVertexAlreadyExist(e)
        case e: SchemaViolationException => recoverVertexAlreadyExist(e)
        case e: Exception =>
          logger.error("error initialising vertex", e)
          Future.failed(VertexCreationException(s"error initialising vertex ${coreVertex.toString}", e))
      }

  }

  /**
    * When adding vertices asynchronously, a vertex or a vertex property can have already been added by another thread or instance
    * Then it's not an actual error that we're catching
    */
  private def recoverVertexAlreadyExist(error: Throwable): Future[Option[Vertex]] = {
    logger.warn("uniqueness constraint, recovering" + error.getMessage)

    searchForVertexByProperties(coreVertex.properties)
      .flatMap {
        case actualVertex @ Some(_) => Future.successful(actualVertex)
        case None => buildAddVertex.map(_.headOption)
      }

  }

  def isPropertyIterable(propertyName: String): Boolean = {
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

