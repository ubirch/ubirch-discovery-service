package com.ubirch.discovery.core

import com.ubirch.discovery.core.connector.{ ConnectorType, GremlinConnector, GremlinConnectorFactory }
import org.scalatest.{ FeatureSpec, Matchers }

class GetVerticesSpec extends FeatureSpec with Matchers {

  implicit val gc: GremlinConnector = GremlinConnectorFactory.getInstance(ConnectorType.JanusGraph)

  feature("get all vertices") {
    scenario("get all") {

      //prepare

    }

    scenario("detDepth") {
      //      GetVertices().getVertexDepth("1", 3)
    }
  }
}
