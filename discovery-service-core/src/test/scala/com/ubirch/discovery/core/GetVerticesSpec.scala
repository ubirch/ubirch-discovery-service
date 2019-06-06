package com.ubirch.discovery.core

import com.ubirch.discovery.core.connector.GremlinConnector
import com.ubirch.discovery.core.operation.GetVertices
import org.scalatest.{ FeatureSpec, Matchers }

class GetVerticesSpec extends FeatureSpec with Matchers {

  implicit val gc: GremlinConnector = GremlinConnector.get

  feature("get all vertices") {
    scenario("get all") {

      //prepare
      new AddVerticesSpec

    }

    scenario("detDepth") {
      new GetVertices().getVertexDepth("1", 3)
    }
  }
}
