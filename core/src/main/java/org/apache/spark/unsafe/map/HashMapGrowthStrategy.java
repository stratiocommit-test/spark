/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.unsafe.map;

/**
 * Interface that defines how we can grow the size of a hash map when it is over a threshold.
 */
public interface HashMapGrowthStrategy {

  int nextCapacity(int currentCapacity);

  /**
   * Double the size of the hash map every time.
   */
  HashMapGrowthStrategy DOUBLING = new Doubling();

  class Doubling implements HashMapGrowthStrategy {
    @Override
    public int nextCapacity(int currentCapacity) {
      assert (currentCapacity > 0);
      // Guard against overflow
      return (currentCapacity * 2 > 0) ? (currentCapacity * 2) : Integer.MAX_VALUE;
    }
  }

}
