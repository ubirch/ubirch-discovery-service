package com.ubirch.discovery.core.structure

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.discovery.core.TestUtil
import gremlin.scala.{Key, KeyValue}
import org.scalatest.{FeatureSpec, Matchers}

class ElementsSpec extends FeatureSpec with Matchers with LazyLogging {

  val label1 = "label1"
  val label2 = "label2"
  val labelEdge = "labelEdge"

  val Number: Key[Any] = Key[Any]("number")
  val Name: Key[Any] = Key[Any]("name")
  val Created: Key[Any] = Key[Any]("created")
  val Test: Key[Any] = Key[Any]("truc")
  val IdAssigned: Key[Any] = Key[Any]("IdAssigned")

  val properties1: List[ElementProperty] = List(
    ElementProperty(KeyValue[Any](Number, 5.toLong), PropertyType.Long),
    ElementProperty(KeyValue[Any](Name, "name1"), PropertyType.String)
  )
  val properties2: List[ElementProperty] = List(
    ElementProperty(KeyValue[Any](Number, 6.toLong), PropertyType.Long),
    ElementProperty(KeyValue[Any](Name, "name2"), PropertyType.String)
  )
  val propertiesEdge: List[ElementProperty] = List(
    ElementProperty(KeyValue[Any](Test, "an Edge"), PropertyType.String)
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

  }

}
