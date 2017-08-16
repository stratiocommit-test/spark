/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark;

import java.io.IOException;
import java.io.Serializable;

import org.junit.After;
import org.junit.Before;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class SharedSparkSession implements Serializable {

  protected transient SparkSession spark;
  protected transient JavaSparkContext jsc;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName(getClass().getSimpleName())
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }
}
