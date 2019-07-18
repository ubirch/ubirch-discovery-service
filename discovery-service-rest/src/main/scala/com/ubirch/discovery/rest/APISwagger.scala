package com.ubirch.discovery.rest

import org.scalatra.ScalatraServlet
import org.scalatra.swagger.{ApiInfo, NativeSwaggerBase, Swagger}

class ResourcesApp(implicit val swagger: Swagger) extends ScalatraServlet with NativeSwaggerBase

object RestApiInfo extends ApiInfo(
  "Ubirch JanusGraph API",
  "Swagger documentation for the Ubirch REST API",
  "http://ubirch.de",
  "benoit.george@ubirch.com",
  "Apache V2",
  "https://www.apache.org/licenses/LICENSE-2.0"
)

class ApiSwagger extends Swagger(Swagger.SpecVersion, "1.0.0", RestApiInfo)
