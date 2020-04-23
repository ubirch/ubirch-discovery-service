package com.ubirch.discovery.core.structure

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.structure.Elements.Property
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import com.ubirch.discovery.core.util.Timer
import gremlin.scala.{ GremlinScala, KeyValue, TraversalSource, Vertex }
import gremlin.scala.GremlinScala.Aux
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.janusgraph.core.SchemaViolationException
import shapeless.HNil

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

class VertexDatabase(val coreVertex: VertexCore, val gc: GremlinConnector)(implicit propSet: Set[Property]) extends LazyLogging {

  override def toString: String = coreVertex.toString

  val g: TraversalSource = gc.g
  val b: Bindings = gc.b

  def getUpdateOrCreate: Vertex = {

    logger.info(s"getUpdateOrCreate(${coreVertex.toString})")
    val t0 = System.currentTimeMillis()

    def createAllPropertiesTraversal(constructor: Aux[Vertex, HNil]): Aux[Vertex, HNil] = {

      var newConstructor = constructor
      for { props <- coreVertex.properties } {
        newConstructor = newConstructor.property(props.toKeyValue)
      }
      newConstructor
    }

    def createWhatWeWant[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] = trav => trav.has(prop)

    val rs: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] = {

      //Seq(trav => trav.has(oneProp))
      val res = coreVertex.properties.filter(p => isPropertyIterable(p.keyName)).map { p => createWhatWeWant(p.toKeyValue) }.toSeq
      res
    }

    val firstConstructor: Aux[Vertex, HNil] = gc.g.V().or(rs: _*).fold().coalesce(_.unfold(), _.addV(coreVertex.label))
    val res = createAllPropertiesTraversal(firstConstructor).l().head

    val t1 = System.currentTimeMillis()
    logger.info(s"getUpdateOrCreate time: ${t1 - t0} ms")
    //def or(traversals: (GremlinScala.Aux[End, HNil] => GremlinScala[_])*)
    res

  }

  val vertex: gremlin.scala.Vertex = getUpdateOrCreate

  @tailrec
  private def searchForVertexByProperties(properties: List[ElementProperty]): gremlin.scala.Vertex = {
    properties match {
      case Nil => null
      case property :: restOfProperties =>
        if (!isPropertyIterable(property.keyName)) searchForVertexByProperties(restOfProperties) else
          g.V().has(property.toKeyValue).headOption() match {
            case Some(v) => v
            case None => searchForVertexByProperties(restOfProperties)
          }
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
    checkOnProps(propSet)

  }

  def existInJanusGraph: Boolean = vertex != null

  /**
    * Adds a vertex in the database with his label and properties.
    */
  def addVertexWithProperties(): Unit = {
    if (existInJanusGraph) throw new ImportToGremlinException("Vertex already exist in the database")
    try {
      logger.debug(s"adding vertex ${coreVertex.toString}")
      //vertex = initialiseVertex
    } catch {
      case e: CompletionException =>
        logger.error(s"Error on adding vertex and its properties ${coreVertex.toString}: ", e)
        throw new ImportToGremlinException(s"Error on adding properties to vertex ${coreVertex.toString}: " + e.getMessage) //TODO: do something
      case e: Exception =>
        logger.error(s"Error on adding vertex and its properties ${coreVertex.toString}: ", e)
        throw e
    }
  }

  private def initialiseVertex: Vertex = {
    Try({
      var constructor = gc.g.addV(coreVertex.label)
      for (prop <- coreVertex.properties) {
        constructor = constructor.property(prop.toKeyValue)
      }
      constructor.l().head
    }) match {
      case Success(value) => value
      case Failure(exception) =>
        exception match {
          case e: CompletionException => recoverVertexAlreadyExist(e)
          case e: SchemaViolationException => recoverVertexAlreadyExist(e)
          case e: Exception =>
            logger.error("error initialising vertex", e)
            throw e
        }
    }
  }

  /**
    * When adding vertices asynchronously, a vertex or a vertex property can have already been added by another thread or instance
    * Then it's not an actual error that we're catching
    */
  private def recoverVertexAlreadyExist(error: Throwable) = {
    logger.warn("uniqueness constraint, recovering" + error.getMessage)
    val v: Option[Vertex] = Option(searchForVertexByProperties(coreVertex.properties))
    v match {
      case Some(actualV) => actualV
      case None =>
        var constructor = gc.g.addV(coreVertex.label)
        for (prop <- coreVertex.properties) {
          constructor = constructor.property(prop.toKeyValue)
        }
        constructor.l().head
    }
  }

  /**
    * Add new properties to vertex
    */
  def update(): Unit = {
    val vMap: Map[String, String] = getPropertiesMap map { kv => kv._1.toString -> kv._2.head.toString }
    val shouldBeProps: Map[String, String] = coreVertex.properties.map { ep => ep.keyName -> ep.value.toString }.toMap

    if (vMap.contains("timestamp")) {
      areTheSame(vMap, shouldBeProps)
    } else {
      if (shouldBeProps.contains("timestamp")) {
        areTheSame(vMap, shouldBeProps)
      }
    }
  }

  // check if the properties of the vertex already in janus and the "new" ones are the same
  // not checking the timestamp as it's always a pain in the ass to cast
  private def areTheSame(currentVertexMap: Map[String, String], shouldBeProps: Map[String, String]): Unit = {
    val shouldBeMap: Map[String, String] = coreVertex.properties.map { ep => ep.keyName -> ep.value.toString }.toMap
    if (!(currentVertexMap - "timestamp" == shouldBeMap - "timestamp")) addNewPropertiesToVertex(vertex)
  }

  private def addNewPropertiesToVertex(vertex: Vertex): Unit = {
    val r = Timer.time({
      var constructor = gc.g.V(vertex)
      for (prop <- coreVertex.properties) {
        constructor = constructor.property(prop.toKeyValue)
      }
      constructor.l().head
    })
    //r.logTimeTaken(s"add properties to vertex with id: ${vertex.id().toString}", criticalTimeMs = 100)
    if (r.result.isFailure) throw new Exception(s"error adding properties on vertex ${vertex.toString}", r.result.failed.get)
  }

  /**
    * Returns a Map<Any, List<Any>> fo the properties. A vertex property can have a list of values, thus why
    * the method is returning this kind of structure.
    *
    * @return A map containing the properties name and respective values of the vertex contained in this structure.
    */
  def getPropertiesMap: Map[Any, List[Any]] = {
    val propertyMapAsJava = g.V(vertex).valueMap.toList().head.asScala.toMap.asInstanceOf[Map[Any, util.ArrayList[Any]]]
    propertyMapAsJava map { x => x._1 -> x._2.asScala.toList }
  }

  def deleteVertex(): Unit = {
    if (existInJanusGraph) g.V(vertex.id).drop().iterate()
  }

  def vertexId: String = vertex.id().toString

}

