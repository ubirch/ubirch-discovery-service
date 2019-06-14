package com.ubirch.discovery.core

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import gremlin.scala._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.{ FeatureSpec, Matchers }

import scala.io.Source

class AddVerticesSpec extends FeatureSpec with Matchers with LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnector.get

  private val dateTimeFormat = ISODateTimeFormat.dateTime()

  val defaultLabelValue: String = "aLabel"

  val Number: Key[String] = Key[String]("number")
  val Name: Key[String] = Key[String]("name")
  val Created: Key[String] = Key[String]("created")
  val IdAssigned: Key[String] = Key[String]("IdAssigned")
  implicit val ordering: (KeyValue[String] => String) => Ordering[KeyValue[String]] = Ordering.by[KeyValue[String], String](_)

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  feature("add vertices") {
    scenario("add two unlinked vertex") {

      // clean database
      deleteDatabase()

      // prepare
      val allReq = readAllFiles("/addVerticesSpec/valid/requests/")
      val r1 = allReq.head
      val (id1, l1, p1) = parseStringVertex(r1.head)
      val (id2, l2, p2) = parseStringVertex(r1(1))
      val (lE, pE) = parseStringEdge(r1(2))
      logger.info(parseStringVertex(r1.head).toString())
      // commit
      AddVertices().addTwoVertices(id1, p1, l1)(id2, p2, l2)(pE, lE)

      // analyse
      //    count number of vertices and edges
      val nbVertices = gc.g.V().count().toSet().head
      val nbEdges = gc.g.E.count().toSet().head
      nbVertices shouldBe 2
      nbEdges shouldBe 1

      //    vertices
      val v1Reconstructed = new VertexStructDb(id1, gc.g)
      val v2Reconstructed = new VertexStructDb(id2, gc.g)

      try {
        AddVertices().verifVertex(v1Reconstructed, p1)
        AddVertices().verifVertex(v2Reconstructed, p2)
        AddVertices().verifEdge(id1, id2, pE)
      } catch {
        case e: ImportToGremlinException =>
          logger.error("", e)
          fail()
      }

    }

    scenario("add vertices that follow the format A-B-C") {
      // clean database
      deleteDatabase()

      // prepare
      val id1 = 1.toString
      val id2 = 2.toString
      val id3 = 3.toString
      val now1 = DateTime.now(DateTimeZone.UTC)
      val p1: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now1)))
      val now2 = DateTime.now(DateTimeZone.UTC)
      val p2: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now2)))
      val now3 = DateTime.now(DateTimeZone.UTC)
      val p3: List[KeyValue[String]] = List(new KeyValue[String](Created, dateTimeFormat.print(now3)))

      val pE: List[KeyValue[String]] = List(
        new KeyValue[String](Name, "edge"),
        new KeyValue[String](Number, "38")
      )

      // commit
      AddVertices().addTwoVertices(id1, p1)(id2, p2)(pE)
      AddVertices().addTwoVertices(id2, p2)(id3, p3)(pE)

      // analyse
      //    count number of vertices & edges
      val nbVertex = gc.g.V().count().toSet().head
      nbVertex shouldBe 3
      val nbEdges = gc.g.E().count().toSet.head
      nbEdges shouldBe 2

      //    reconstruct vertices
      val v1Reconstructed = new VertexStructDb(id1.toString, gc.g)
      val v2Reconstructed = new VertexStructDb(id2.toString, gc.g)
      val v3Reconstructed = new VertexStructDb(id3.toString, gc.g)

      //    verify vertices and edges
      try {
        AddVertices().verifVertex(v1Reconstructed, p1)
        AddVertices().verifVertex(v2Reconstructed, p2)
        AddVertices().verifVertex(v3Reconstructed, p3)
        AddVertices().verifEdge(id1, id2, pE)
        AddVertices().verifEdge(id2, id3, pE)
      } catch {
        case e: ImportToGremlinException =>
          logger.error("", e)
          fail()
      }
    }
  }

  feature("verify verifier") {
    scenario("add vertices, verify correct data -> should be TRUE") {
      // no need to implement it, scenario("add two unlinked vertex") already covers this topic
    }

    scenario("add vertices, verify incorrect vertex properties data -> should be FALSE") {
      // clean database
      deleteDatabase()

      // prepare
      val id1 = 1.toString
      val id2 = 2.toString

      val now1 = DateTime.now(DateTimeZone.UTC)
      val p1: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "5"),
        new KeyValue[String](Name, "aName1"),
        new KeyValue[String](Created, dateTimeFormat.print(now1))
      )
      val now2 = DateTime.now(DateTimeZone.UTC)
      val p2: List[KeyValue[String]] = List(
        new KeyValue[String](Number, "6"),
        new KeyValue[String](Name, "aName2"),
        new KeyValue[String](Created, dateTimeFormat.print(now2))
      )
      val pE: List[KeyValue[String]] = List(
        new KeyValue[String](Name, "edge")
      )

      // commit
      AddVertices().addTwoVertices(id1, p1)(id2, p2)(pE)

      // create false data
      val pFalse: List[KeyValue[String]] = List(new KeyValue[String](Number, "1"))

      // analyse
      //    count number of vertices and edges
      val nbVertices = gc.g.V().count().toSet().head

      val nbEdges = gc.g.E.count().toSet().head
      nbVertices shouldBe 2
      nbEdges shouldBe 1
      //    vertices
      val v1Reconstructed = new VertexStructDb(id1, gc.g)

      try {
        AddVertices().verifVertex(v1Reconstructed, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _ => fail
      }

      try {
        AddVertices().verifEdge(id1, id2, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _ => fail
      }

      try {
        AddVertices().verifEdge(id1, id1, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _ => fail
      }
    }
  }

  //TODO: make a test for verifEdge and verifVertex

  // ----------- helpers -----------

  def readAllFiles(directory: String): List[List[String]] = {
    val listFiles = getFilesInDirectory(directory)
    listFiles map { f => readFile(f.getCanonicalPath) }
  }

  def getFilesInDirectory(dir: String): List[File] = {
    val path = getClass.getResource(dir)
    val folder = new File(path.getPath)
    val res: List[File] = if (folder.exists && folder.isDirectory) {
      folder.listFiles
        .toList
    } else Nil
    res
  }

  def readFile(nameOfFile: String): List[String] = {
    val source = Source.fromFile(nameOfFile)
    val lines = source.getLines().toList
    source.close
    lines
  }

  def parseStringVertex(vertexStruct: String): (String, String, List[KeyValue[String]]) = {
    val listValues = vertexStruct.split(";").toList
    assert(listValues.length > 1, s"Test is incorrect: $vertexStruct is missing some values")
    val id = listValues.head
    val label = listValues(1)
    // determine if vertex has properties
    if (listValues.length > 2) {
      val listProperties = extractProps(Nil, listValues.drop(2))
      (id, label, listProperties)
    } else (id, label, Nil)
  }

  def parseStringEdge(edgeStruct: String): (String, List[KeyValue[String]]) = {
    val listValues = edgeStruct.split(";").toList
    assert(listValues.nonEmpty, s"Test is incorrect: $edgeStruct is missing some values")
    val label = listValues.head
    if (listValues.length > 1) {
      val listProps = extractProps(Nil, listValues.drop(1))
      (label, listProps)
    } else (label, Nil)
  }

  def extractProps(accu: List[KeyValue[String]], list: List[String]): List[KeyValue[String]] = {
    list match {
      case Nil => accu
      case x :: xs => {
        val kvAsListOfTwoString = x.split(":")
        val kv = new KeyValue[String](new Key[String](kvAsListOfTwoString.head), kvAsListOfTwoString(1))
        extractProps(kv :: accu, xs)
      }
    }
  }
}
