/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.InternalOutputModes;

/**
 * :: Experimental ::
 *
 * OutputMode is used to what data will be written to a streaming sink when there is
 * new data available in a streaming DataFrame/Dataset.
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
public class OutputMode {

  /**
   * OutputMode in which only the new rows in the streaming DataFrame/Dataset will be
   * written to the sink. This output mode can be only be used in queries that do not
   * contain any aggregation.
   *
   * @since 2.0.0
   */
  public static OutputMode Append() {
    return InternalOutputModes.Append$.MODULE$;
  }

  /**
   * OutputMode in which all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time there are some updates. This output mode can only be used in queries
   * that contain aggregations.
   *
   * @since 2.0.0
   */
  public static OutputMode Complete() {
    return InternalOutputModes.Complete$.MODULE$;
  }
}
