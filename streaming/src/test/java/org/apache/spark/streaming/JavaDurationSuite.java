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

public class JavaDurationSuite {

  // Just testing the methods that are specially exposed for Java.
  // This does not repeat all tests found in the Scala suite.

  @Test
  public void testLess() {
    Assert.assertTrue(new Duration(999).less(new Duration(1000)));
  }

  @Test
  public void testLessEq() {
    Assert.assertTrue(new Duration(1000).lessEq(new Duration(1000)));
  }

  @Test
  public void testGreater() {
    Assert.assertTrue(new Duration(1000).greater(new Duration(999)));
  }

  @Test
  public void testGreaterEq() {
    Assert.assertTrue(new Duration(1000).greaterEq(new Duration(1000)));
  }

  @Test
  public void testPlus() {
    Assert.assertEquals(new Duration(1100), new Duration(1000).plus(new Duration(100)));
  }

  @Test
  public void testMinus() {
    Assert.assertEquals(new Duration(900), new Duration(1000).minus(new Duration(100)));
  }

  @Test
  public void testTimes() {
    Assert.assertEquals(new Duration(200), new Duration(100).times(2));
  }

  @Test
  public void testDiv() {
    Assert.assertEquals(200.0, new Duration(1000).div(new Duration(5)), 1.0e-12);
  }

  @Test
  public void testMilliseconds() {
    Assert.assertEquals(new Duration(100), Durations.milliseconds(100));
  }

  @Test
  public void testSeconds() {
    Assert.assertEquals(new Duration(30 * 1000), Durations.seconds(30));
  }

  @Test
  public void testMinutes() {
    Assert.assertEquals(new Duration(2 * 60 * 1000), Durations.minutes(2));
  }

}
