/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package scala.tools.nsc

import org.apache.spark.annotation.DeveloperApi

// NOTE: Forced to be public (and in scala.tools.nsc package) to access the
//       settings "explicitParentLoader" method

/**
 * Provides exposure for the explicitParentLoader method on settings instances.
 */
@DeveloperApi
object SparkHelper {
  /**
   * Retrieves the explicit parent loader for the provided settings.
   *
   * @param settings The settings whose explicit parent loader to retrieve
   *
   * @return The Optional classloader representing the explicit parent loader
   */
  @DeveloperApi
  def explicitParentLoader(settings: Settings) = settings.explicitParentLoader
}
