package com.ubirch.discovery.models

import com.ubirch.discovery.models.Elements.Property
import gremlin.scala.Vertex

trait VertexMap {
  def get(vertexCore: VertexCore): Option[Vertex]

  def contains(vertexCore: VertexCore): Boolean

  def size: Int
}

case class DefaultVertexMap(map: Map[VertexCore, Vertex])(implicit val propSet: Set[Property]) extends VertexMap {

  override def get(vertexCore: VertexCore): Option[Vertex] = {
    if (map.contains(vertexCore)) {
      Some(map.filter(v => v._1.equalsUniqueProperty(vertexCore)).head._2)
    } else {
      None
    }
  }

  override def contains(vertexCore: VertexCore): Boolean = map.keys.exists(v => v.equalsUniqueProperty(vertexCore))

  override def size: Int = map.size
}
