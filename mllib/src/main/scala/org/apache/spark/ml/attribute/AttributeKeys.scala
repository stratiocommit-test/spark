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

/**
 * Keys used to store attributes.
 */
private[attribute] object AttributeKeys {
  val ML_ATTR: String = "ml_attr"
  val TYPE: String = "type"
  val NAME: String = "name"
  val INDEX: String = "idx"
  val MIN: String = "min"
  val MAX: String = "max"
  val STD: String = "std"
  val SPARSITY: String = "sparsity"
  val ORDINAL: String = "ord"
  val VALUES: String = "vals"
  val NUM_VALUES: String = "num_vals"
  val ATTRIBUTES: String = "attrs"
  val NUM_ATTRIBUTES: String = "num_attrs"
}
