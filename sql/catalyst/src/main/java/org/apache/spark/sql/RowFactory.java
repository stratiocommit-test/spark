/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * A factory class used to construct {@link Row} objects.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
public class RowFactory {

  /**
   * Create a {@link Row} from the given arguments. Position i in the argument list becomes
   * position i in the created {@link Row} object.
   *
   * @since 1.3.0
   */
  public static Row create(Object ... values) {
    return new GenericRow(values);
  }
}
