package com.ubirch.discovery.core

import com.ubirch.discovery.core.structure.Elements
import gremlin.scala.KeyValue

object TestUtil {

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
