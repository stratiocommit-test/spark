/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.unsafe;

/**
 * Class to make changes to record length offsets uniform through out
 * various areas of Apache Spark core and unsafe.  The SPARC platform
 * requires this because using a 4 byte Int for record lengths causes
 * the entire record of 8 byte Items to become misaligned by 4 bytes.
 * Using a 8 byte long for record length keeps things 8 byte aligned.
 */
public class UnsafeAlignedOffset {

  private static final int UAO_SIZE = Platform.unaligned() ? 4 : 8;

  public static int getUaoSize() {
    return UAO_SIZE;
  }

  public static int getSize(Object object, long offset) {
    switch (UAO_SIZE) {
      case 4:
        return Platform.getInt(object, offset);
      case 8:
        return (int)Platform.getLong(object, offset);
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }

  public static void putSize(Object object, long offset, int value) {
    switch (UAO_SIZE) {
      case 4:
        Platform.putInt(object, offset, value);
        break;
      case 8:
        Platform.putLong(object, offset, value);
        break;
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }
}
