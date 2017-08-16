/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher

import java.util.{List => JList, Map => JMap}

/**
 * Exposes AbstractCommandBuilder to the YARN tests, so that they can build classpaths the same
 * way other cluster managers do.
 */
private[spark] class TestClasspathBuilder extends AbstractCommandBuilder {

  childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sys.props("spark.test.home"))

  override def buildClassPath(extraCp: String): JList[String] = super.buildClassPath(extraCp)

  /** Not used by the YARN tests. */
  override def buildCommand(env: JMap[String, String]): JList[String] =
    throw new UnsupportedOperationException()

}
