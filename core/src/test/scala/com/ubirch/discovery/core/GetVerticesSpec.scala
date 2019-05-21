package com.ubirch.discovery.core

import org.scalatest.{FeatureSpec, Matchers}

class GetVerticesSpec extends FeatureSpec with Matchers {

  feature("get all vertices") {
    scenario("get all") {

      //prepare
      val stuff = new AddVerticesSpec

    }

    scenario("detDepth") {
      GetVertices.getVertexDepth("1", 3)
    }
  }
}
