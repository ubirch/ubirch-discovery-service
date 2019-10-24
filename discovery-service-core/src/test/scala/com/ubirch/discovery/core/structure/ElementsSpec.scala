package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.TestUtil
import gremlin.scala.{Key, KeyValue}
import org.scalatest.{FeatureSpec, Matchers}

class ElementsSpec extends FeatureSpec with Matchers with LazyLogging {

  val label1 = "label1"
  val label2 = "label2"
  val labelEdge = "labelEdge"

  val Number: Key[String] = Key[String]("number")
  val Name: Key[String] = Key[String]("name")
  val Created: Key[String] = Key[String]("created")
  val Test: Key[String] = Key[String]("truc")
  val IdAssigned: Key[String] = Key[String]("IdAssigned")

  val properties1: List[KeyValue[String]] = List(
    new KeyValue[String](Number, "5"),
    new KeyValue[String](Name, "name1"),
  )
  val properties2: List[KeyValue[String]] = List(
    new KeyValue[String](Number, "6"),
    new KeyValue[String](Name, "name2"),
  )
  val propertiesEdge: List[KeyValue[String]] = List(
    new KeyValue[String](Test, "an Edge")
  )

  feature("relation class") {
    scenario("toString") {


      implicit val propSet: Set[Elements.Property] = TestUtil.putPropsOnPropSet(properties1)

      val v1 = VertexCore(properties1, label1)
      val v2 = VertexCore(properties2, label2)
      val e = EdgeCore(propertiesEdge, labelEdge)

      val relation = Relation(v1, v2, e)
      logger.info(relation.toString)
    }

    scenario("delete this") {
      def generateTime = {
        val t = System.currentTimeMillis
        s"""g.addV("testVertex").property("timestamp", $t)"""
      }
      while(true) {
        println(generateTime)
        Thread.sleep(1100)
      }
    }
  }

}
