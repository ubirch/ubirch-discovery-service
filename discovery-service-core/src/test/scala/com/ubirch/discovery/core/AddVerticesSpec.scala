package com.ubirch.discovery.core

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.{ConnectorType, GremlinConnector, GremlinConnectorFactory}
import com.ubirch.discovery.core.operation.AddVertices
import com.ubirch.discovery.core.structure._
import com.ubirch.discovery.core.util.Exceptions.ImportToGremlinException
import gremlin.scala._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{FeatureSpec, Matchers}

import scala.io.Source

class AddVerticesSpec extends FeatureSpec with Matchers with LazyLogging {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.Test)

  def deleteDatabase(): Unit = {
    gc.g.V().drop().iterate()
  }

  feature("add vertices - correct tests") {

    def executeTestValid(relations: List[Relation], testConfiguration: TestConfValid) = {
      // clean
      deleteDatabase()
      // commit
      relations foreach { relation =>
        implicit val propSet: Set[Elements.Property] = putPropsOnPropSet(relation.vFrom.properties) ++ putPropsOnPropSet(relation.vTo.properties)
        AddVertices().createRelation(relation)
      }
      // verif
      relations foreach { relation =>
        implicit val propSet: Set[Elements.Property] =  putPropsOnPropSet(relation.vFrom.properties) ++ putPropsOnPropSet(relation.vTo.properties)

        try {
          verificationRelation(relation)
        } catch {
          case e: Throwable =>
            logger.error("", e)
            fail()
        }
      }
      val nbVertices = gc.g.V().count().toSet().head
      val nbEdges = gc.g.E.count().toSet().head
      (nbVertices, nbEdges) shouldBe (testConfiguration.nbVertex, testConfiguration.nbEdges)
    }

    // get all the test data
    val allReq: List[(List[String], TestConfValid)] = readAllFilesValid("/addVerticesSpec/valid/basic/")

    // format test data
    val listAllElems: List[(List[Relation], TestConfValid)] = allReq map { vve: (List[String], TestConfValid) =>
      getRelations((Nil, TestConfValid(0, 0, "")), vve)
    }

    // run the tests
    listAllElems foreach { m =>
      scenario(m._2.nameTest) {
        executeTestValid(m._1, m._2)
      }
    }
  }

  feature("add vertices - properties can be updated") {

    def executeTestValid(relations: List[Relation], testConfValid: TestConfValid): Unit = {
      // clean
      deleteDatabase()
      // commit
      relations foreach { relation =>
        implicit val propSet: Set[Elements.Property] = putPropsOnPropSet(relation.vFrom.properties) ++ putPropsOnPropSet(relation.vTo.properties)

        AddVertices().createRelation(relation)


        // verif
        try {
          verificationRelation(relation)

        } catch {
          case e: Throwable =>
          logger.error("", e)
            fail()
        }
      }

      val nbVertices = gc.g.V().count().toSet().head
      val nbEdges = gc.g.E.count().toSet().head
      (nbVertices, nbEdges) shouldBe (testConfValid.nbVertex, testConfValid.nbEdges)
    }

    // get all the test data
    val allReq: List[(List[String], TestConfValid)] = readAllFilesValid("/addVerticesSpec/valid/updateProps/")
    // format test data
    val listAllElems: List[(List[Relation], TestConfValid)] = allReq map { vve: (List[String], TestConfValid) =>
      getRelations((Nil, TestConfValid(0, 0, "")), vve)
    }
    listAllElems foreach { m =>
      scenario(m._2.nameTest) {
        executeTestValid(m._1, m._2)
      }
    }
  }

  feature("add vertices - incorrect tests") {

    def executeTestInvalid(listCoupleVAndE: List[Relation], testConfInvalid: TestConfInvalid): Unit = {
      deleteDatabase()

      // commit
      logger.info("Testing " + testConfInvalid.nameTest)
      listCoupleVAndE foreach { relationTest =>
        try {
          implicit val propSet: Set[Elements.Property] = putPropsOnPropSet(relationTest.vFrom.properties) ++ putPropsOnPropSet(relationTest.vTo.properties)

          AddVertices().createRelation(relationTest)
        } catch {
          case e: ImportToGremlinException =>
            logger.info(e.getMessage)
            e.getMessage shouldBe testConfInvalid.expectedResult
          case e: Throwable =>
            logger.error("", e)
            fail()
        }
      }
    }

    // get all the test data
    val allReq: List[(List[String], TestConfInvalid)] = readAllFilesInvalid("/addVerticesSpec/invalid/")

    // format test data
    val listAllElems: List[(List[Relation], TestConfInvalid)] = allReq map { vve: (List[String], TestConfInvalid) =>
      getRelations((Nil, TestConfInvalid("", "")), vve)
    }

    // run the tests
    listAllElems foreach { m =>
      scenario(m._2.nameTest) {
        executeTestInvalid(m._1, m._2)
      }
    }

  }

  feature("verify verifier") {
    scenario("add vertices, verify correct data -> should be TRUE") {
      // no need to implement it, scenario("add two unlinked vertex") already covers this topic
    }

    scenario("add vertices, verify incorrect vertex properties data -> should be FALSE") {

      val dateTimeFormat = ISODateTimeFormat.dateTime()

      val Number: Key[String] = Key[String]("number")
      val Name: Key[String] = Key[String]("name")
      val Created: Key[String] = Key[String]("created")
      implicit val ordering: (KeyValue[String] => String) => Ordering[KeyValue[String]] = Ordering.by[KeyValue[String], String](_)

      // clean database
      deleteDatabase()

      // prepare

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

      val internalVertexFrom = VertexCore(p1, "aLabel")
      val internalVertexTo = VertexCore(p2, "aLabel")
      val internalEdge = EdgeCore(pE, "aLabel")
      val relation = Relation(internalVertexFrom, internalVertexTo, internalEdge)
      // commit
      implicit val propSet: Set[Elements.Property] = putPropsOnPropSet(p1)
      AddVertices().createRelation(relation)

      // create false data
      val pFalse: List[KeyValue[String]] = List(new KeyValue[String](Number, "1"))

      // analyse
      //    count number of vertices and edges
      val nbVertices = gc.g.V().count().toSet().head

      val nbEdges = gc.g.E.count().toSet().head
      nbVertices shouldBe 2
      nbEdges shouldBe 1
      //    vertices
      val v1Reconstructed = internalVertexFrom.toVertexStructDb(gc.g)
      val v2Reconstructed = internalVertexTo.toVertexStructDb(gc.g)
      try {
        AddVertices().verifVertex(v1Reconstructed, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _: Throwable => fail
      }

      try {
        AddVertices().verifEdge(v1Reconstructed, v2Reconstructed, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _: Throwable => fail
      }

      try {
        AddVertices().verifEdge(v1Reconstructed, v1Reconstructed, pFalse)
        fail
      } catch {
        case _: ImportToGremlinException =>
        case _: Throwable => fail
      }
    }
  }

  // ----------- helpers -----------

  def verificationRelation(relation: Relation)(implicit propSet: Set[Elements.Property]): Unit = {
    val vFromDb = relation.vFrom.toVertexStructDb(gc.g)
    val vToDb = relation.vTo.toVertexStructDb(gc.g)
    AddVertices().verifVertex(vFromDb, relation.vFrom.properties)
    AddVertices().verifVertex(vToDb, relation.vTo.properties)
    AddVertices().verifEdge(vFromDb, vToDb, relation.edge.properties)
  }

  def getRelations[T](accu: (List[Relation], T), toParse: (List[String], T)): (List[Relation], T) = {
    toParse match {
      case (Nil, _) => accu
      case x =>
        getRelations(accu = (accu._1 :+ Relation(stringToVertex(x._1.head), stringToVertex(x._1(1)), StringToEdge(x._1(2))), x._2), toParse = (x._1.drop(3), x._2))
    }
  }

  def readAllFilesValid(directory: String): List[(List[String], TestConfValid)] = {
    val filesExpectedResults = getFilesInDirectory(directory + "expectedResults/")
    val filesReq = getFilesInDirectory(directory + "requests/")

    val allExpectedResults: List[TestConfValid] = filesExpectedResults map { f =>
      val bothInt = readFile(f.getCanonicalPath).head
      val nbVertex = bothInt.substring(0, bothInt.indexOf(",")).toInt
      val nbEdges = bothInt.substring(bothInt.indexOf(",") + 1).toInt
      TestConfValid(nbVertex, nbEdges, f.getName)
    }
    val allReq = filesReq map { f => readFile(f.getCanonicalPath) }
    allReq zip allExpectedResults
  }


  def readAllFilesInvalid(directory: String): List[(List[String], TestConfInvalid)] = {
    val filesExpectedResults = getFilesInDirectory(directory + "expectedResults/")
    val filesReq = getFilesInDirectory(directory + "requests/")

    val allExpectedResults: List[TestConfInvalid] = filesExpectedResults map { f =>
      val errorMsg = readFile(f.getCanonicalPath).head
      TestConfInvalid(errorMsg, f.getName)
    }
    val allReq = filesReq map { f => readFile(f.getCanonicalPath) }

    allReq zip allExpectedResults
  }


  def stringToVertex(vertexStruct: String): VertexCore = {
    val listValues = vertexStruct.split(";").toList
    assert(listValues.nonEmpty, s"Test is incorrect: $vertexStruct is missing some values")
    val label = listValues.head
    val listProperties = extractProps(Nil, listValues.tail)
    VertexCore(listProperties, label)

  }

  case class TestConfValid(nbVertex: Int, nbEdges: Int, nameTest: String)

  case class TestConfInvalid(expectedResult: String, nameTest: String)

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


  def StringToEdge(edgeStruct: String): EdgeCore = {
    val listValues = edgeStruct.split(";").toList
    assert(listValues.nonEmpty, s"Test is incorrect: $edgeStruct is missing some values")
    val label = listValues.head
    if (listValues.length > 1) {
      val listProps = extractProps(Nil, listValues.drop(1))
      EdgeCore(listProps, label)
    } else EdgeCore(Nil, label)
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

  def putPropsOnPropSet(propList: List[KeyValue[String]]): Set[Elements.Property] = {
    def iterateOnListProp(it: List[KeyValue[String]], accu: Set[Elements.Property]): Set[Elements.Property] = {
      it match {
        case Nil => accu
        case x :: xs => iterateOnListProp(xs, accu ++ Set(new Elements.Property(x.key.name, true)))
      }
    }

    iterateOnListProp(propList, Set())
  }
}
