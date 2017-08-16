/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis._

/**
 * Interface for configuration options used in the catalyst module.
 */
trait CatalystConf {
  def caseSensitiveAnalysis: Boolean

  def orderByOrdinal: Boolean
  def groupByOrdinal: Boolean

  def optimizerMaxIterations: Int
  def optimizerInSetConversionThreshold: Int
  def maxCaseBranchesForCodegen: Int

  def runSQLonFile: Boolean

  def warehousePath: String

  /** If true, cartesian products between relations will be allowed for all
   * join types(inner, (left|right|full) outer).
   * If false, cartesian products will require explicit CROSS JOIN syntax.
   */
  def crossJoinEnabled: Boolean

  /**
   * Returns the [[Resolver]] for the current configuration, which can be used to determine if two
   * identifiers are equal.
   */
  def resolver: Resolver = {
    if (caseSensitiveAnalysis) caseSensitiveResolution else caseInsensitiveResolution
  }
}


/** A CatalystConf that can be used for local testing. */
case class SimpleCatalystConf(
    caseSensitiveAnalysis: Boolean,
    orderByOrdinal: Boolean = true,
    groupByOrdinal: Boolean = true,
    optimizerMaxIterations: Int = 100,
    optimizerInSetConversionThreshold: Int = 10,
    maxCaseBranchesForCodegen: Int = 20,
    runSQLonFile: Boolean = true,
    crossJoinEnabled: Boolean = false,
    warehousePath: String = "/user/hive/warehouse")
  extends CatalystConf
