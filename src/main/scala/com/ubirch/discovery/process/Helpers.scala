package com.ubirch.discovery.process

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.models.{ DumbRelation, VertexCore }
import com.ubirch.discovery.services.connector.GremlinConnector
import gremlin.scala.{ Edge, GremlinScala, KeyValue, StepLabel, Vertex }
import gremlin.scala.GremlinScala.Aux
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet
import org.janusgraph.core.SchemaViolationException
import shapeless.HNil

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }
import scala.collection.JavaConverters._

object Helpers extends LazyLogging {

  // will create something like
  // g.V().or(__.has('hash', 'a'), __.has('signature', '1')).fold().coalesce(unfold(), addV('person')).aggregate('a').property('hash', 'a').property('signature', '1')
  // .V().or(__.has('hash', 'b'), __.has('signature', '2')).fold().coalesce(unfold(), addV('person')).aggregate('b').property('hash', 'b').property('signature', '2')
  // .V().or(__.has('hash', 'c'), __.has('signature', '3')).fold().coalesce(unfold(), addV('person')).aggregate('c').property('hash', 'c').property('signature', '3')
  // .V().or(__.has('hash', 'c')).fold().coalesce(unfold(), addV('person')).aggregate('d').property('hash', 'c').property('signature', '3')
  // .select('a', 'b', 'c', 'd')
  def getUpdateOrCreateVertices(verticesCore: List[VertexCore])(implicit gc: GremlinConnector, ec: ExecutionContext, propSet: Set[Property]): Map[VertexCore, Vertex] = {

    // case that if only one or two vertex is present, then finishTraversal(verticeAccu.traversable, verticeAccu.getStepLabels).l().head.asScala
    // will be cast to something that doesn't work. This is due to the way the select() method work in scala
    if (verticesCore.size == 1 || verticesCore.size == 2) {
      verticesCore.map(vc => vc -> getUpdateOrCreateSingle(vc)).toMap
    } else {

      val t0 = System.currentTimeMillis()

      def initTraversal(vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]]): Aux[Vertex, HNil] = {
        // helper function to make the has() step in the or() step
        def createWhatWeWant[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] = trav => trav.has(prop)

        // create a list of has(property), has(property) that will be used inside the or step
        val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
          vertexCore.properties.filter(p => isPropertyIterable(p.keyName)).map { p => createWhatWeWant(p.toKeyValue) }.toSeq

        // will property().[..].property steps to the constructor
        def addPropertiesToTraversal(constructor: Aux[Vertex, HNil]): Aux[Vertex, HNil] = {
          var newConstructor = constructor
          for { props <- vertexCore.properties } {
            newConstructor = newConstructor.property(props.toKeyValue)
          }
          newConstructor
        }
        val firstPartOfQuery = gc.g.V().or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
        addPropertiesToTraversal(firstPartOfQuery)
      }

      // will add a or(_.has(), ..., _.has()).fold().coalesce(unfold(), addV(label)).aggregate(value).prop().[..].prop() to the previous constructor
      def createTraversalForOne(previousConstructor: GremlinScala.Aux[Vertex, HNil], vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]]): Aux[Vertex, HNil] = {

        // helper function to make the has() step in the or() step
        def createWhatWeWant[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] =
          trav => trav.has(prop)

        // create a list of has(property), has(property) that will be used inside the or step
        val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
          vertexCore.properties.filter(p => isPropertyIterable(p.keyName)).map { p => createWhatWeWant(p.toKeyValue) }.toSeq

        // will property().[..].property steps to the constructor
        def addPropertiesToTraversal(constructor: Aux[Vertex, HNil]): Aux[Vertex, HNil] = {
          var newConstructor = constructor
          for { props <- vertexCore.properties } {
            newConstructor = newConstructor.property(props.toKeyValue)
          }
          newConstructor
        }

        val firstPartOfQuery = previousConstructor.V().or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
        addPropertiesToTraversal(firstPartOfQuery)

      }

      /*
      Will add .select(aggregatedValues) to the traversal
      The scala gremlin library has a strange behaviour, which forces us to do the following trick
       */
      def finishTraversal(almostFinishedTraversal: Aux[Vertex, HNil], allAggregatedValues: List[StepLabel[java.util.Set[Vertex]]]) = {

        val t: List[String] = allAggregatedValues.map(p => p.name)
        if (t.size == 1) {
          almostFinishedTraversal.select[java.util.Map[String, Any]](t.head)
        } else if (t.size == 2) {
          almostFinishedTraversal.select(t.head, t.tail.head)
        } else {
          almostFinishedTraversal.select(t.head, t.tail.head, t.tail.tail: _*)
        }
      }

      case class VerticeAccu(verticeAndStep: Map[StepLabel[java.util.Set[Vertex]], VertexCore], traversable: Aux[Vertex, HNil]) {

        def addNewVertex(vertexCore: VertexCore): VerticeAccu = {
          val stepLabel = StepLabel[java.util.Set[Vertex]]()
          copy(verticeAndStep + (stepLabel -> vertexCore), createTraversalForOne(traversable, vertexCore, stepLabel))
        }

        def getStepLabels: List[StepLabel[util.Set[Vertex]]] = verticeAndStep.keySet.toList

      }

      var verticeAccu: VerticeAccu = {
        val stepLabel = StepLabel[java.util.Set[Vertex]]()
        VerticeAccu(Map(stepLabel -> verticesCore.head), initTraversal(verticesCore.head, stepLabel))
      }

      for { v <- verticesCore.tail } {
        verticeAccu = verticeAccu.addNewVertex(v)
      }

      val traversalRes: mutable.Map[String, Any] = finishTraversal(verticeAccu.traversable, verticeAccu.getStepLabels).l().head.asScala

      val t1 = System.currentTimeMillis()
      // for tests, print

      // print totalNumber,timeTakenProcessAll,timeTakenIndividually
      //logger.info(s"getUpdateOrCreateVertices:[${verticesCore.size},${t1 - t0},${(t1 - t0).toDouble / verticesCore.size.toDouble}]")

      verticeAccu.verticeAndStep.map(sl => sl._2 -> traversalRes(sl._1.name).asInstanceOf[BulkSet[Vertex]].iterator().next())

    }

  }

  def getUpdateOrCreateSingle(vertexCore: VertexCore)(implicit gc: GremlinConnector, ec: ExecutionContext, propSet: Set[Property]): Vertex = {

    def createAllPropertiesTraversal(constructor: Aux[Vertex, HNil]): Aux[Vertex, HNil] = {

      var newConstructor = constructor
      for { props <- vertexCore.properties } {
        newConstructor = newConstructor.property(props.toKeyValue)
      }
      newConstructor
    }

    def createWhatWeWant[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] = trav => trav.has(prop)

    val rs: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
      vertexCore.properties.filter(p => isPropertyIterable(p.keyName)).map { p => createWhatWeWant(p.toKeyValue) }.toSeq

    val firstConstructor: Aux[Vertex, HNil] = gc.g.V().or(rs: _*).fold().coalesce(_.unfold(), _.addV(vertexCore.label))
    val t0 = System.currentTimeMillis()
    val res = createAllPropertiesTraversal(firstConstructor).l().head
    val t1 = System.currentTimeMillis()
    logger.debug(s"getUpdateOrCreateSingle:[1,${t1 - t0},${t1 - t0}]")

    res

  }

  def createRelation(relation: DumbRelation)(implicit gc: GremlinConnector): Unit = {
    Try(createEdge(relation)) match {
      case Success(edge) => edge
      case Failure(fail) => fail match {
        case e: CompletionException => recoverEdge(relation, e)
        case e: SchemaViolationException => recoverEdge(relation, e)
        case e: Exception =>
          logger.error("error initialising vertex", e)
          throw e
      }
    }
  }

  private def createEdge(relation: DumbRelation)(implicit gc: GremlinConnector): Aux[Edge, HNil] = {
    if (relation.edge.properties.isEmpty) {
      gc.g.V(relation.vTo).addE(relation.edge.label).from(relation.vFrom).iterate()
    } else {
      var constructor = gc.g.V(relation.vTo).addE(relation.edge.label)
      for (prop <- relation.edge.properties) {
        constructor = constructor.property(prop.toKeyValue)
      }
      constructor.from(relation.vFrom).iterate()
    }
  }

  private def recoverEdge(relation: DumbRelation, error: Throwable)(implicit gc: GremlinConnector) {
    if (!error.getMessage.contains("An edge with the given label already exists between the pair of vertices and the label")) {
      createEdge(relation)
    }

  }

  private def isPropertyIterable(propertyName: String)(implicit propSet: Set[Property]): Boolean = {

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

}
