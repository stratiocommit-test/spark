/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.unsafe.array;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryBlock;

public class LongArraySuite {

  @Test
  public void basicTest() {
    long[] bytes = new long[2];
    LongArray arr = new LongArray(MemoryBlock.fromLongArray(bytes));
    arr.set(0, 1L);
    arr.set(1, 2L);
    arr.set(1, 3L);
    Assert.assertEquals(2, arr.size());
    Assert.assertEquals(1L, arr.get(0));
    Assert.assertEquals(3L, arr.get(1));

    arr.zeroOut();
    Assert.assertEquals(0L, arr.get(0));
    Assert.assertEquals(0L, arr.get(1));
  }
}
