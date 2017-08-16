/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.unsafe.Platform;

/**
 * Simulates Hive's hashing function at
 * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#hashcode()
 */
public class HiveHasher {

  @Override
  public String toString() {
    return HiveHasher.class.getSimpleName();
  }

  public static int hashInt(int input) {
    return input;
  }

  public static int hashLong(long input) {
    return (int) ((input >>> 32) ^ input);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
    assert (lengthInBytes >= 0): "lengthInBytes cannot be negative";
    int result = 0;
    for (int i = 0; i < lengthInBytes; i++) {
      result = (result * 31) + (int) Platform.getByte(base, offset + i);
    }
    return result;
  }
}
