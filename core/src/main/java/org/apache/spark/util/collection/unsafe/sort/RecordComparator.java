/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.collection.unsafe.sort;

/**
 * Compares records for ordering. In cases where the entire sorting key can fit in the 8-byte
 * prefix, this may simply return 0.
 */
public abstract class RecordComparator {

  /**
   * Compare two records for order.
   *
   * @return a negative integer, zero, or a positive integer as the first record is less than,
   *         equal to, or greater than the second.
   */
  public abstract int compare(
    Object leftBaseObject,
    long leftBaseOffset,
    Object rightBaseObject,
    long rightBaseOffset);
}
