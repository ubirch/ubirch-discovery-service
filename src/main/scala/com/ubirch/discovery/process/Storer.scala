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
import javax.inject.{ Inject, Singleton }

trait Storer {

  /**
    * Method that will get, update or create the vertices contained in verticesCore and return a map of the
    * VertexCore linked to the graph Vertex
    * The function will first try to get the vertex. If successful, it'll update it, otherwise it'll create it
    * In order to get a vertex from the graph, only the properties defined in the propSet and that are iterable
    * will be used.
    * @param verticesCore The list of vertices and their properties that will be evaluated
    * @param propSet A list of properties.
    * @return A map making the relation between the vertexCore passed in argument and their reference in the graph.
    */
  def getUpdateOrCreateVertices(verticesCore: List[VertexCore])(implicit propSet: Set[Property]): Map[VertexCore, Vertex]

  /**
    * Same as getUpdateOrCreateVertices, but for a single vertex
    * @param vertexCore The vertex and its properties that will be used to create, get or update its reference on
    *                   the graph
    * @param propSet A list of properties.
    * @return The graph vertex.
    */
  def getUpdateOrCreateSingle(vertexCore: VertexCore)(implicit propSet: Set[Property]): Vertex

  /**
    * Will create an edge in the graph between the two given vertex in the relation, with the properties described in
    * the edge.
    */
  def createRelation(relation: DumbRelation): Unit
}

@Singleton
class DefaultJanusgraphStorer @Inject() (gremlinConnector: GremlinConnector, ec: ExecutionContext) extends Storer with LazyLogging {

  implicit val gc = gremlinConnector
  // will create something like
  // g.V().or(__.has('hash', 'a'), __.has('signature', '1')).fold().coalesce(unfold(), addV('person')).aggregate('a').property('hash', 'a').property('signature', '1')
  // .V().or(__.has('hash', 'b'), __.has('signature', '2')).fold().coalesce(unfold(), addV('person')).aggregate('b').property('hash', 'b').property('signature', '2')
  // .V().or(__.has('hash', 'c'), __.has('signature', '3')).fold().coalesce(unfold(), addV('person')).aggregate('c').property('hash', 'c').property('signature', '3')
  // .V().or(__.has('hash', 'c')).fold().coalesce(unfold(), addV('person')).aggregate('d').property('hash', 'c').property('signature', '3')
  // .select('a', 'b', 'c', 'd')
  def getUpdateOrCreateVertices(verticesCore: List[VertexCore])(implicit propSet: Set[Property]): Map[VertexCore, Vertex] = {

    // case that if only one or two vertex is present, then finishTraversal(verticeAccu.traversable, verticeAccu.getStepLabels).l().head.asScala
    // will be cast to something that doesn't work. This is due to the way the select() method work in scala
    if (verticesCore.size == 1 || verticesCore.size == 2) {
      verticesCore.map(vc => vc -> getUpdateOrCreateSingle(vc)).toMap
    } else {

      /**
        * Will add the list of properties contained in vertexCore to the already existing constructor
        * @param constructor A gremlin traversal that will be expanded to contain the rest of the queries
        * @param vertexCore The representation of the vertex
        * @return An updated gremlin traversal query
        */
      def addVertexPropertiesToTraversal(constructor: Aux[Vertex, HNil], vertexCore: VertexCore): Aux[Vertex, HNil] = {
        var newConstructor = constructor
        for { props <- vertexCore.properties } {
          newConstructor = newConstructor.property(props.toKeyValue)
        }
        newConstructor
      }

      /**
        * helper function that adds the has() step to a traversal
        */
      def addHasStepToTraversal[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] =
        trav => trav.has(prop)

      def initTraversal(vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]]): Aux[Vertex, HNil] = {

        // create a list of has(property), has(property) that will be used inside the or step
        // DO NOT remove the .toSeq, even id intellij tells you to
        val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
          vertexCore.properties.filter(p => isPropertyIterable(p.keyName)).map { p => addHasStepToTraversal(p.toKeyValue) }.toSeq

        val firstPartOfQuery = gc.g.V().or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
        addVertexPropertiesToTraversal(firstPartOfQuery, vertexCore)
      }

      // will add a or(_.has(), ..., _.has()).fold().coalesce(unfold(), addV(label)).aggregate(value).prop().[..].prop() to the previous constructor
      def addTraversalForOneToExistingTraversable(previousConstructor: GremlinScala.Aux[Vertex, HNil], vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]]): Aux[Vertex, HNil] = {

        // create a list of has(property), has(property) that will be used inside the or step
        val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
          vertexCore.properties.filter(p => isPropertyIterable(p.keyName)).map { p => addHasStepToTraversal(p.toKeyValue) }.toSeq

        val firstPartOfQuery = previousConstructor.V().or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
        addVertexPropertiesToTraversal(firstPartOfQuery, vertexCore)

      }

      /**
        * Will add .select(aggregatedValues) to the traversal
        * The scala gremlin library has a strange behaviour, which forces us to do the following trick
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

      /**
        * This class is used to keep track of which step label correspond to which step label
        * The logic of initializing the traversal is kept outside of this class
        * Using the addNewVertex will add a or(_.has(), ..., _.has()).fold().coalesce(unfold(), addV(label)).aggregate(value).prop().[..].prop()
        * step to the current traversal constructor
        *
        * @param verticeAndStep
        * @param traversal initialized traversal
        */
      case class VerticeAccu(verticeAndStep: Map[StepLabel[java.util.Set[Vertex]], VertexCore], traversal: Aux[Vertex, HNil]) {

        def addNewVertex(vertexCore: VertexCore): VerticeAccu = {
          val stepLabel = StepLabel[java.util.Set[Vertex]]()
          copy(verticeAndStep + (stepLabel -> vertexCore), addTraversalForOneToExistingTraversable(traversal, vertexCore, stepLabel))
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

      val completeTraversal = finishTraversal(verticeAccu.traversal, verticeAccu.getStepLabels)
      val traversalRes: mutable.Map[String, Any] = completeTraversal.l().head.asScala

      verticeAccu.verticeAndStep.map(sl => sl._2 -> traversalRes(sl._1.name).asInstanceOf[BulkSet[Vertex]].iterator().next())

    }

  }

  def getUpdateOrCreateSingle(vertexCore: VertexCore)(implicit propSet: Set[Property]): Vertex = {

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

  def createRelation(relation: DumbRelation): Unit = {
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
