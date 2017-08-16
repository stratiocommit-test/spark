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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.QueryTest

class HiveUtilsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("newTemporaryConfiguration overwrites listener configurations") {
    Seq(true, false).foreach { useInMemoryDerby =>
      val conf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby)
      assert(conf(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname) === "")
    }
  }
}
