package com.ubirch.discovery.core

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure.VertexStructDb
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import gremlin.scala._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FeatureSpec, Matchers}

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
    scenario("valid scenarios") {

      // clean database
      deleteDatabase()

      // prepare
      val allReq: List[(List[String], (Int, Int))] = readAllFiles("/addVerticesSpec/valid/")

      val listAllElems: List[(List[CoupleVAndE], (Int, Int))] = allReq map { vve: (List[String], (Int, Int)) =>

        def getListVVE(accu: (List[CoupleVAndE], (Int, Int)), toParse: (List[String], (Int, Int))): (List[CoupleVAndE], (Int, Int)) = {
          toParse match {
            case (Nil, _) => accu
            case x =>
              getListVVE((CoupleVAndE(stringToVert(x._1.head), stringToVert(x._1(1)), StringToEdg(x._1(2))) :: accu._1, x._2), (x._1.drop(3), x._2))
          }
        }
        getListVVE((Nil, (0, 0)), vve)
      }

      listAllElems foreach { lVVE: (List[CoupleVAndE], (Int, Int)) =>
        // clean
        deleteDatabase()
        // commit
        lVVE._1 foreach { vve =>
          AddVertices().addTwoVertices(vve.v1.id, vve.v1.props, vve.v1.label)(vve.v2.id, vve.v2.props, vve.v2.label)(vve.e.props, vve.e.label)
        }
        // verif
        lVVE._1 foreach { vve =>

          //    vertices
          val v1Reconstructed = new VertexStructDb(vve.v1.id, gc.g)
          val v2Reconstructed = new VertexStructDb(vve.v2.id, gc.g)

          try {
            AddVertices().verifVertex(v1Reconstructed, vve.v1.props)
            AddVertices().verifVertex(v2Reconstructed, vve.v2.props)
            AddVertices().verifEdge(vve.v1.id, vve.v2.id, vve.e.props)
          } catch {
            case e: ImportToGremlinException =>
              logger.error("", e)
              fail()
          }
        }
        val nbVertices = gc.g.V().count().toSet().head
        val nbEdges = gc.g.E.count().toSet().head
        (nbVertices, nbEdges) shouldBe lVVE._2

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
  case class CoupleVAndE(v1: Vert, v2: Vert, e: Edg)
  case class Vert(id: String, label: String, props: List[KeyValue[String]])
  case class Edg(label: String, props: List[KeyValue[String]])

  def readAllFiles(directory: String): List[(List[String], (Int, Int))] = {
    val filesExpectedResults = getFilesInDirectory(directory + "expectedResults/")
    val filesReq = getFilesInDirectory(directory + "requests/")

    val allExpectedResults: List[(Int, Int)] = filesExpectedResults map { f =>
      val bothInt = readFile(f.getCanonicalPath).head
      val nbVertex = bothInt.substring(0, bothInt.indexOf(",")).toInt
      val nbEdges = bothInt.substring(bothInt.indexOf(",") + 1).toInt
      (nbVertex, nbEdges)
    }
    val allReq = filesReq map { f => readFile(f.getCanonicalPath) }

    logger.info((allReq zip allExpectedResults) mkString ", ")

    allReq zip allExpectedResults
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

  def stringToVert(vertexStruct: String): Vert = {
    val listValues = vertexStruct.split(";").toList
    assert(listValues.length > 1, s"Test is incorrect: $vertexStruct is missing some values")
    val id = listValues.head
    val label = listValues(1)
    // determine if vertex has properties
    if (listValues.length > 2) {
      val listProperties = extractProps(Nil, listValues.drop(2))
      Vert(id, label, listProperties)
    } else Vert(id, label, Nil)
  }

  def StringToEdg(edgeStruct: String): Edg = {
    val listValues = edgeStruct.split(";").toList
    assert(listValues.nonEmpty, s"Test is incorrect: $edgeStruct is missing some values")
    val label = listValues.head
    if (listValues.length > 1) {
      val listProps = extractProps(Nil, listValues.drop(1))
      Edg(label, listProps)
    } else Edg(label, Nil)
  }

  def extractProps(accu: List[KeyValue[String]], list: List[String]): List[KeyValue[String]] = {
    list match {
      case Nil => accu
      case x :: xs =>
        val kvAsListOfTwoString = x.split(":")
        val kv = new KeyValue[String](new Key[String](kvAsListOfTwoString.head), kvAsListOfTwoString(1))
        extractProps(kv :: accu, xs)
    }
  }
}