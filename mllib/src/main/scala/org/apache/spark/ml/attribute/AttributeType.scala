/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.attribute

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * An enum-like type for attribute types: [[AttributeType$#Numeric]], [[AttributeType$#Nominal]],
 * and [[AttributeType$#Binary]].
 */
@DeveloperApi
sealed abstract class AttributeType(val name: String)

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object AttributeType {

  /** Numeric type. */
  val Numeric: AttributeType = {
    case object Numeric extends AttributeType("numeric")
    Numeric
  }

  /** Nominal type. */
  val Nominal: AttributeType = {
    case object Nominal extends AttributeType("nominal")
    Nominal
  }

  /** Binary type. */
  val Binary: AttributeType = {
    case object Binary extends AttributeType("binary")
    Binary
  }

  /** Unresolved type. */
  val Unresolved: AttributeType = {
    case object Unresolved extends AttributeType("unresolved")
    Unresolved
  }

  /**
   * Gets the [[AttributeType]] object from its name.
   * @param name attribute type name: "numeric", "nominal", or "binary"
   */
  def fromName(name: String): AttributeType = {
    if (name == Numeric.name) {
      Numeric
    } else if (name == Nominal.name) {
      Nominal
    } else if (name == Binary.name) {
      Binary
    } else if (name == Unresolved.name) {
      Unresolved
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $name.")
    }
  }
}
