/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.internal

import org.apache.spark.internal.config._

/**
 * A helper class that enables substitution using syntax like
 * `${var}`, `${system:var}` and `${env:var}`.
 *
 * Variable substitution is controlled by `SQLConf.variableSubstituteEnabled`.
 */
class VariableSubstitution(conf: SQLConf) {

  private val provider = new ConfigProvider {
    override def get(key: String): Option[String] = Option(conf.getConfString(key, ""))
  }

  private val reader = new ConfigReader(provider)
    .bind("spark", provider)
    .bind("sparkconf", provider)
    .bind("hivevar", provider)
    .bind("hiveconf", provider)

  /**
   * Given a query, does variable substitution and return the result.
   */
  def substitute(input: String): String = {
    if (conf.variableSubstituteEnabled) {
      reader.substitute(input)
    } else {
      input
    }
  }
}
