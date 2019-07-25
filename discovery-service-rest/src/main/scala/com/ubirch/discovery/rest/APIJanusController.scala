package com.ubirch.discovery.rest

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.GetVertices
import com.ubirch.discovery.core.structure.VertexStruct
import com.ubirch.discovery.core.util.Util.arrayVertexToJson
import gremlin.scala.{Key, KeyValue}
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.NativeJsonSupport
import org.scalatra.swagger.{ResponseMessage, Swagger, SwaggerSupport, SwaggerSupportSyntax}
import org.scalatra.{CorsSupport, ScalatraServlet}

import scala.language.postfixOps

class APIJanusController(implicit val swagger: Swagger) extends ScalatraServlet
  with NativeJsonSupport with SwaggerSupport with CorsSupport with LazyLogging {

  // Allows CORS support to display the swagger UI when using the same network
  options("/*") {
    response.setHeader(
      "Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers")
    )
  }

  implicit val gc: GremlinConnector = GremlinConnector.get

  // Stops the APIJanusController from being abstract
  protected val applicationDescription = "The API working with JanusGraph, allows add / display of vertexes/edges"

  // Sets up automatic case class to JSON output serialization
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  // Before every action runs, set the content type to be in JSON format.
  before() {
    contentType = formats("json")
  }

  val addToJanus: SwaggerSupportSyntax.OperationBuilder =
    (apiOperation[AddVertices]("addToJanusTwoVertexes")
      summary "Add two to JanusGraph"
      schemes "http" // Force swagger ui to use http instead of https, only need to say it once
      description "Still not implemented. Does not work right now as it should now support dynamic properties addition"
      parameters (
        pathParam[String]("label1").
        description("label of the first vertex"),
        queryParam[Option[Map[String, String]]]("properties1").
        description("Properties of the second vertex"),
        pathParam[String]("label2").
        description("label of the second vertex"),
        queryParam[Option[Map[String, String]]]("properties2").
        description("Properties of the second vertex"),
        pathParam[String]("labelEdge").
        description("label of the edge"),
        queryParam[Option[Map[String, String]]]("propertiesEdge").
        description("Properties of the edge that link the two vertexes")
      ))

  post("/addVertexToJG/:id1/:id2", operation(addToJanus)) {
    println(params.get("properties1"))

    def propertiesToKeyValuesList(propName: String): List[KeyValue[String]] = {

      def extractMapFromString(propName: String): Map[String, String] = {
        val properties = params.getOrElse(propName, "")
        val jValue = parse(properties)

        if (jValue == JNothing) {
          Map.empty[String, String]
        } else {
          logger.info(jValue.extract[Map[String, String]].mkString(", "))
          jValue.extract[Map[String, String]]
        }

      }

      extractMapFromString(propName) map { x => KeyValue(Key(x._1), x._2) } toList
    }

    val prop1 = propertiesToKeyValuesList("properties1")
    val prop2 = propertiesToKeyValuesList("properties2")
    val propE = propertiesToKeyValuesList("propertiesEdge")
    val label1 = params("label1")
    val label2 = params("label2")
    val labelEdge = params("labelEdge")
    //val res: String = AddVertices().addTwoVertices(prop1, label1)(prop2, label2)(propE, labelEdge)
    "TODO: fix this"
  }

  val getVertices: SwaggerSupportSyntax.OperationBuilder =
    (apiOperation[List[VertexStruct]]("getVertexesJanusGraph")
      summary "Display informations about a Vertex"
      description "Display informations about a Vertex (ID and properties)." +
      "Not providing an ID will display the entire database"
      parameter queryParam[Option[Int]]("id").description("Id of the vertex we're looking for")
      responseMessage ResponseMessage(404, "404: Can't find edge with the given ID"))

  get("/getVertices", operation(getVertices)) {
    params.get("id") match {
      case Some(id) =>
        val vertex = GetVertices().getVertexByPublicId(id)
        if (vertex == null) {
          halt(404, s"404: Can't find vertex with the ID: $id")
        } else {
          vertex.toJson
        }
      case None =>
        val listVertexes = GetVertices().getAllVertices(100)
        arrayVertexToJson(listVertexes.toArray)
    }
  }

  val getVerticesWithDepth: SwaggerSupportSyntax.OperationBuilder =
    (apiOperation[List[VertexWithDepth]]("getVertexesWithDepth")
      summary "Get a vertex and the surrounding ones"
      description "see summary"
      parameter queryParam[String]("id").description("Id of the vertex we're looking for")
      parameter queryParam[Int]("depth").description("Depth of what we're looking for")

      responseMessage ResponseMessage(404, "404: Can't find edge with the ID: idNumber"))

  get("/getVertexesDepth", operation(getVerticesWithDepth)) {

    val neighbors = GetVertices().getVertexDepth(params.get("id").get, params.get("depth").get.toInt)
    if (neighbors == null) {
      halt(404, s"404: Can't find vertex with the provided ID")
    } else {
      Serialization.write(neighbors)
    }

  }

}

case class VertexWithDepth(distance: Array[Integer])

case class AddVertices(id1: Int, properties1: Map[String, String], id2: Integer, properties2: Map[String, String], propertiesEdge: Map[String, String])
