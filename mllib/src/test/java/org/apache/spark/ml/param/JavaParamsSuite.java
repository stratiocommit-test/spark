/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.param;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Param and related classes in Java
 */
public class JavaParamsSuite {

  @Test
  public void testParams() {
    JavaTestParams testParams = new JavaTestParams();
    Assert.assertEquals(testParams.getMyIntParam(), 1);
    testParams.setMyIntParam(2).setMyDoubleParam(0.4).setMyStringParam("a");
    Assert.assertEquals(testParams.getMyDoubleParam(), 0.4, 0.0);
    Assert.assertEquals(testParams.getMyStringParam(), "a");
    Assert.assertArrayEquals(testParams.getMyDoubleArrayParam(), new double[]{1.0, 2.0}, 0.0);
  }

  @Test
  public void testParamValidate() {
    ParamValidators.gt(1.0);
    ParamValidators.gtEq(1.0);
    ParamValidators.lt(1.0);
    ParamValidators.ltEq(1.0);
    ParamValidators.inRange(0, 1, true, false);
    ParamValidators.inRange(0, 1);
    ParamValidators.inArray(Arrays.asList(0, 1, 3));
    ParamValidators.inArray(Arrays.asList("a", "b"));
  }
}
