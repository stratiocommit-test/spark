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

/**
 * SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
public enum SaveMode {
  /**
   * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
   * contents of the DataFrame are expected to be appended to existing data.
   *
   * @since 1.3.0
   */
  Append,
  /**
   * Overwrite mode means that when saving a DataFrame to a data source,
   * if data/table already exists, existing data is expected to be overwritten by the contents of
   * the DataFrame.
   *
   * @since 1.3.0
   */
  Overwrite,
  /**
   * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
   * an exception is expected to be thrown.
   *
   * @since 1.3.0
   */
  ErrorIfExists,
  /**
   * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
   * the save operation is expected to not save the contents of the DataFrame and to not
   * change the existing data.
   *
   * @since 1.3.0
   */
  Ignore
}
