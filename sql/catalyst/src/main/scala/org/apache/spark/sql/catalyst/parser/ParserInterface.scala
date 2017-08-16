/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Interface for a parser.
 */
trait ParserInterface {
  /** Creates LogicalPlan for a given SQL string. */
  def parsePlan(sqlText: String): LogicalPlan

  /** Creates Expression for a given SQL string. */
  def parseExpression(sqlText: String): Expression

  /** Creates TableIdentifier for a given SQL string. */
  def parseTableIdentifier(sqlText: String): TableIdentifier
}
