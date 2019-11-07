package com.ubirch.discovery.core

import com.ubirch.discovery.core.structure.{ElementProperty, Elements}

object TestUtil {

  def putPropsOnPropSet(propList: List[ElementProperty]): Set[Elements.Property] = {
    propList.map { prop => new Elements.Property(prop.keyName, true) }.toSet
  }

  def isAllDigits(x: String): Boolean = x forall Character.isDigit

}
