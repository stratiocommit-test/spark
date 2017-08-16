/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming;

import org.junit.Assert;
import org.junit.Test;

public class JavaTimeSuite {

  // Just testing the methods that are specially exposed for Java.
  // This does not repeat all tests found in the Scala suite.

  @Test
  public void testLess() {
    Assert.assertTrue(new Time(999).less(new Time(1000)));
  }

  @Test
  public void testLessEq() {
    Assert.assertTrue(new Time(1000).lessEq(new Time(1000)));
  }

  @Test
  public void testGreater() {
    Assert.assertTrue(new Time(1000).greater(new Time(999)));
  }

  @Test
  public void testGreaterEq() {
    Assert.assertTrue(new Time(1000).greaterEq(new Time(1000)));
  }

  @Test
  public void testPlus() {
    Assert.assertEquals(new Time(1100), new Time(1000).plus(new Duration(100)));
  }

  @Test
  public void testMinusTime() {
    Assert.assertEquals(new Duration(900), new Time(1000).minus(new Time(100)));
  }

  @Test
  public void testMinusDuration() {
    Assert.assertEquals(new Time(900), new Time(1000).minus(new Duration(100)));
  }

}
