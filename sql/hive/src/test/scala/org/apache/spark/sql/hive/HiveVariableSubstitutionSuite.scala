/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveVariableSubstitutionSuite extends QueryTest with TestHiveSingleton {
  test("SET hivevar with prefix") {
    spark.sql("SET hivevar:county=gram")
    assert(spark.conf.getOption("county") === Some("gram"))
  }

  test("SET hivevar with dotted name") {
    spark.sql("SET hivevar:eloquent.mosquito.alphabet=zip")
    assert(spark.conf.getOption("eloquent.mosquito.alphabet") === Some("zip"))
  }

  test("hivevar substitution") {
    spark.conf.set("pond", "bus")
    checkAnswer(spark.sql("SELECT '${hivevar:pond}'"), Row("bus") :: Nil)
  }

  test("variable substitution without a prefix") {
    spark.sql("SET hivevar:flask=plaid")
    checkAnswer(spark.sql("SELECT '${flask}'"), Row("plaid") :: Nil)
  }

  test("variable substitution precedence") {
    spark.conf.set("turn.aloof", "questionable")
    spark.sql("SET hivevar:turn.aloof=dime")
    // hivevar clobbers the conf setting
    checkAnswer(spark.sql("SELECT '${turn.aloof}'"), Row("dime") :: Nil)
  }
}
