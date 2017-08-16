/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

import scala.Tuple2;

import org.junit.Test;

public class JavaVectorsSuite {

  @Test
  public void denseArrayConstruction() {
    Vector v = Vectors.dense(1.0, 2.0, 3.0);
    assertArrayEquals(new double[]{1.0, 2.0, 3.0}, v.toArray(), 0.0);
  }

  @Test
  public void sparseArrayConstruction() {
    @SuppressWarnings("unchecked")
    Vector v = Vectors.sparse(3, Arrays.asList(
      new Tuple2<>(0, 2.0),
      new Tuple2<>(2, 3.0)));
    assertArrayEquals(new double[]{2.0, 0.0, 3.0}, v.toArray(), 0.0);
  }
}
