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
import java.util.List;

import org.apache.spark.ml.util.Identifiable$;

/**
 * A subclass of Params for testing.
 */
public class JavaTestParams extends JavaParams {

  public JavaTestParams() {
    this.uid_ = Identifiable$.MODULE$.randomUID("javaTestParams");
    init();
  }

  public JavaTestParams(String uid) {
    this.uid_ = uid;
    init();
  }

  private String uid_;

  @Override
  public String uid() {
    return uid_;
  }

  private IntParam myIntParam_;

  public IntParam myIntParam() {
    return myIntParam_;
  }

  public int getMyIntParam() {
    return (Integer) getOrDefault(myIntParam_);
  }

  public JavaTestParams setMyIntParam(int value) {
    set(myIntParam_, value);
    return this;
  }

  private DoubleParam myDoubleParam_;

  public DoubleParam myDoubleParam() {
    return myDoubleParam_;
  }

  public double getMyDoubleParam() {
    return (Double) getOrDefault(myDoubleParam_);
  }

  public JavaTestParams setMyDoubleParam(double value) {
    set(myDoubleParam_, value);
    return this;
  }

  private Param<String> myStringParam_;

  public Param<String> myStringParam() {
    return myStringParam_;
  }

  public String getMyStringParam() {
    return getOrDefault(myStringParam_);
  }

  public JavaTestParams setMyStringParam(String value) {
    set(myStringParam_, value);
    return this;
  }

  private DoubleArrayParam myDoubleArrayParam_;

  public DoubleArrayParam myDoubleArrayParam() {
    return myDoubleArrayParam_;
  }

  public double[] getMyDoubleArrayParam() {
    return getOrDefault(myDoubleArrayParam_);
  }

  public JavaTestParams setMyDoubleArrayParam(double[] value) {
    set(myDoubleArrayParam_, value);
    return this;
  }

  private void init() {
    myIntParam_ = new IntParam(this, "myIntParam", "this is an int param", ParamValidators.gt(0));
    myDoubleParam_ = new DoubleParam(this, "myDoubleParam", "this is a double param",
      ParamValidators.inRange(0.0, 1.0));
    List<String> validStrings = Arrays.asList("a", "b");
    myStringParam_ = new Param<>(this, "myStringParam", "this is a string param",
      ParamValidators.inArray(validStrings));
    myDoubleArrayParam_ =
      new DoubleArrayParam(this, "myDoubleArrayParam", "this is a double param");

    setDefault(myIntParam(), 1);
    setDefault(myDoubleParam(), 0.5);
    setDefault(myDoubleArrayParam(), new double[]{1.0, 2.0});
  }

  @Override
  public JavaTestParams copy(ParamMap extra) {
    return defaultCopy(extra);
  }
}
