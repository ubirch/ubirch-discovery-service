package com.ubirch.discovery.models

import java.util
import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.models.Elements.Property
import com.ubirch.discovery.services.connector.GremlinConnector
import gremlin.scala.{ Edge, GremlinScala, KeyValue, StepLabel, Vertex }
import gremlin.scala.GremlinScala.Aux
import javax.inject.{ Inject, Singleton }
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet
import org.janusgraph.core.SchemaViolationException
import shapeless.HNil

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

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
  def getUpdateOrCreateVerticesConcrete(verticesCore: List[VertexCore])(implicit propSet: Set[Property]): Map[VertexCore, Vertex]

  /**
    * Same as getUpdateOrCreateVertices, but for a single vertex
    * @param vertexCore The vertex and its properties that will be used to create, get or update its reference on
    *                   the graph
    * @param propSet A list of properties.
    * @return The graph vertex.
    */
  def getUpdateOrCreateSingleConcrete(vertexCore: VertexCore)(implicit propSet: Set[Property]): Vertex

  /**
    * Will create an edge in the graph between the two given vertex in the relation, with the properties described in
    * the edge.
    */
  def createRelation(relation: DumbRelation): Unit
}

object GremlinTraversalExtension {

  /**
    * This class is used to keep track of which step label correspond to which step label
    * The logic of initializing the traversal is kept outside of this class
    * Using the addNewVertex will add a or(_.has(), ..., _.has()).fold().coalesce(unfold(), addV(label)).aggregate(value).prop().[..].prop()
    * step to the current traversal constructor
    *
    * @param verticeAndStep
    * @param traversal initialized traversal
    */
  case class VerticeAccu(verticeAndStep: Map[StepLabel[java.util.Set[Vertex]], VertexCore], traversal: Aux[Vertex, HNil])(implicit propSet: Set[Property]) {

    def addNewVertex(vertexCore: VertexCore): VerticeAccu = {
      val stepLabel = StepLabel[java.util.Set[Vertex]]()
      copy(verticeAndStep + (stepLabel -> vertexCore), addTraversalForOneToExistingTraversable(traversal, vertexCore, stepLabel))
    }

    def getStepLabels: List[StepLabel[util.Set[Vertex]]] = verticeAndStep.keySet.toList

  }

  // will add a or(_.has(), ..., _.has()).fold().coalesce(unfold(), addV(label)).aggregate(value).prop().[..].prop() to the previous constructor
  private def addTraversalForOneToExistingTraversable(previousConstructor: GremlinScala.Aux[Vertex, HNil], vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]])(implicit propSet: Set[Property]): Aux[Vertex, HNil] = {

    // create a list of has(property), has(property) that will be used inside the or step
    val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
      vertexCore.properties.filter(p => p.isUnique).map { p => addHasStepToTraversal(p.toKeyValue) }.toSeq

    val firstPartOfQuery = previousConstructor.V().or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
    addVertexPropertiesToTraversal(firstPartOfQuery, vertexCore)

  }

  /**
    * Will add the list of properties contained in vertexCore to the already existing constructor
    * @param constructor A gremlin traversal that will be expanded to contain the rest of the queries
    * @param vertexCore The representation of the vertex
    * @return An updated gremlin traversal query
    */
  private def addVertexPropertiesToTraversal(constructor: Aux[Vertex, HNil], vertexCore: VertexCore): Aux[Vertex, HNil] = {
    var newConstructor = constructor
    for { props <- vertexCore.properties } {
      newConstructor = newConstructor.property(props.toKeyValue)
    }
    newConstructor
  }

  /**
    * helper function that adds the has() step to a traversal
    */
  private def addHasStepToTraversal[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] =
    trav => trav.has(prop)

  implicit class RichTraversal(val previousConstructor: GremlinScala.Aux[Vertex, HNil]) extends AnyVal {

    /*
    * will create something like
    * g.V().or(__.has('hash', 'a'), __.has('signature', '1')).fold().coalesce(unfold(), addV('person')).aggregate('a').property('hash', 'a').property('signature', '1')
    * .V().or(__.has('hash', 'b'), __.has('signature', '2')).fold().coalesce(unfold(), addV('person')).aggregate('b').property('hash', 'b').property('signature', '2')
    * .V().or(__.has('hash', 'c'), __.has('signature', '3')).fold().coalesce(unfold(), addV('person')).aggregate('c').property('hash', 'c').property('signature', '3')
    * .V().or(__.has('hash', 'c')).fold().coalesce(unfold(), addV('person')).aggregate('d').property('hash', 'c').property('signature', '3')
    * .select('a', 'b', 'c', 'd')
    */
    def getUpdateOrCreateVertices(verticesCore: List[VertexCore])(implicit propSet: Set[Property]): (Aux[util.Map[String, Any], HNil], VerticeAccu) = {

      def initTraversal(vertexCore: VertexCore, aggregateValue: StepLabel[java.util.Set[Vertex]]): Aux[Vertex, HNil] = {

        // create a list of has(property), has(property) that will be used inside the or step
        // DO NOT remove the .toSeq, even if intellij tells you to
        val hasPropertySteps: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
          vertexCore.properties.filter(p => p.isUnique).map { p => addHasStepToTraversal(p.toKeyValue) }.toSeq

        val firstPartOfQuery = previousConstructor.or(hasPropertySteps: _*).fold().coalesce(_.unfold[Vertex](), _.addV(vertexCore.label)).aggregate(aggregateValue)
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

      var verticeAccu: VerticeAccu = {
        val stepLabel = StepLabel[java.util.Set[Vertex]]()
        VerticeAccu(Map(stepLabel -> verticesCore.head), initTraversal(verticesCore.head, stepLabel))
      }

      for { v <- verticesCore.tail } {
        verticeAccu = verticeAccu.addNewVertex(v)
      }

      val completeTraversal = finishTraversal(verticeAccu.traversal, verticeAccu.getStepLabels)

      (completeTraversal, verticeAccu)
    }

    def getUpdateOrCreateSingle(vertexCore: VertexCore)(implicit propSet: Set[Property]): Aux[Vertex, HNil] = {

      def createAllPropertiesTraversal(constructor: Aux[Vertex, HNil]): Aux[Vertex, HNil] = {

        var newConstructor = constructor
        for { props <- vertexCore.properties } {
          newConstructor = newConstructor.property(props.toKeyValue)
        }
        newConstructor
      }

      def createWhatWeWant[Any](prop: KeyValue[Any]): GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil] = trav => trav.has(prop)

      val rs: Seq[GremlinScala.Aux[Vertex, HNil] => GremlinScala.Aux[Vertex, HNil]] =
        vertexCore.properties.filter(p => p.isUnique).map { p => createWhatWeWant(p.toKeyValue) }.toSeq

      val firstConstructor: Aux[Vertex, HNil] = previousConstructor.or(rs: _*).fold().coalesce(_.unfold(), _.addV(vertexCore.label))
      createAllPropertiesTraversal(firstConstructor)

    }

    def createEdge(relation: DumbRelation)(implicit gc: GremlinConnector): Aux[Edge, HNil] = {
      if (relation.edge.properties.isEmpty) {
        previousConstructor.V(relation.vTo).addE(relation.edge.label).from(relation.vFrom)
      } else {
        var constructor = gc.g.V(relation.vTo).addE(relation.edge.label)
        for (prop <- relation.edge.properties) {
          constructor = constructor.property(prop.toKeyValue)
        }
        constructor.from(relation.vFrom)
      }
    }
  }
}

@Singleton
class DefaultJanusgraphStorer @Inject() (gremlinConnector: GremlinConnector, ec: ExecutionContext) extends Storer with LazyLogging {

  implicit val gc = gremlinConnector

  import GremlinTraversalExtension.RichTraversal

  /**
    * Concrete implementation. Will use the methods defined in GremlinTraversalExtension.RichTraversal to complete the
    * traversal. This method only execute the traversal
    *
    * @param verticesCore The list of vertices and their properties that will be evaluated
    * @param propSet      A list of properties.
    * @return A map making the relation between the vertexCore passed in argument and their reference in the graph.
    */
  def getUpdateOrCreateVerticesConcrete(verticesCore: List[VertexCore])(implicit propSet: Set[Property]): Map[VertexCore, Vertex] = {

    // case that if only one or two vertex is present, then finishTraversal(verticeAccu.traversable, verticeAccu.getStepLabels).l().head.asScala
    // will be cast to something that doesn't work. This is due to the way the select() method work in scala
    if (verticesCore.size == 1 || verticesCore.size == 2) {
      verticesCore.map(vc => vc -> getUpdateOrCreateSingleConcrete(vc)).toMap
    } else {

      val (traversal, verticeAccu) = gc.g.V().getUpdateOrCreateVertices(verticesCore)

      try {
        val finalTraversal: mutable.Map[String, Any] = traversal.l().head.asScala
        verticeAccu.verticeAndStep.map(sl => sl._2 -> finalTraversal(sl._1.name).asInstanceOf[BulkSet[Vertex]].iterator().next())
      } catch {
        case e: java.util.concurrent.CompletionException =>
          logger.info("Uniqueness prop error, trying again", e.getMessage)
          try {
            val finalTraversal: mutable.Map[String, Any] = traversal.l().head.asScala
            verticeAccu.verticeAndStep.map(sl => sl._2 -> finalTraversal(sl._1.name).asInstanceOf[BulkSet[Vertex]].iterator().next())
          } catch {
            case e: java.util.concurrent.CompletionException =>
              logger.warn("Uniqueness prop error AGAIN, returning null preprocess hashmap", e.getMessage)
              Map.empty
            case e: Throwable =>
              logger.error("error getUpdateOrCreateVerticesConcrete AGAIN", e)
              throw e
          }
        case e: Throwable =>
          logger.error("error getUpdateOrCreateVerticesConcrete", e)
          throw e
      }
    }

  }

  /**
    * Concrete implementation. Will use the methods defined in GremlinTraversalExtension.RichTraversal to complete the
    * * traversal. This method only execute the traversal
    *
    * @param vertexCore The vertex and its properties that will be used to create, get or update its reference on
    *                   the graph
    * @param propSet    A list of properties.
    * @return The graph vertex.
    */
  def getUpdateOrCreateSingleConcrete(vertexCore: VertexCore)(implicit propSet: Set[Property]): Vertex = {


    try {
      gc.g.V().getUpdateOrCreateSingle(vertexCore).l().head
    } catch {
      case e: java.util.concurrent.CompletionException =>
        logger.info("Uniqueness prop error, trying again", e.getMessage)
        try {
          gc.g.V().getUpdateOrCreateSingle(vertexCore).l().head
        } catch {
          case e: java.util.concurrent.CompletionException =>
            logger.warn("Uniqueness prop error AGAIN, returning null preprocess hashmap", e.getMessage)
            null
          case e: Throwable =>
            logger.error("error getUpdateOrCreateVerticesConcrete AGAIN", e)
            throw e
        }
      case e: Throwable =>
        logger.error("error getUpdateOrCreateVerticesConcrete", e)
        throw e
    }

  }

  def createRelation(relation: DumbRelation): Unit = {
    Try(createEdgeConcrete(relation)) match {
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

  private def createEdgeConcrete(relation: DumbRelation)(implicit gc: GremlinConnector): Aux[Edge, HNil] = {
    gc.g.V(relation.vTo).createEdge(relation).iterate()
  }

  private def recoverEdge(relation: DumbRelation, error: Throwable)(implicit gc: GremlinConnector) {
    if (!error.getMessage.contains("An edge with the given label already exists between the pair of vertices and the label")) {
      createEdgeConcrete(relation)
    }
  }

}
