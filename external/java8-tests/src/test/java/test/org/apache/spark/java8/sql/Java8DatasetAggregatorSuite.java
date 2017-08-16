/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package test.org.apache.spark.java8.sql;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.expressions.javalang.typed;
import test.org.apache.spark.sql.JavaDatasetAggregatorSuiteBase;

/**
 * Suite that replicates tests in JavaDatasetAggregatorSuite using lambda syntax.
 */
public class Java8DatasetAggregatorSuite extends JavaDatasetAggregatorSuiteBase {
  @Test
  public void testTypedAggregationAverage() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> agged = grouped.agg(typed.avg(v -> (double)(v._2() * 2)));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3.0), tuple2("b", 6.0)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationCount() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> agged = grouped.agg(typed.count(v -> v));
    Assert.assertEquals(Arrays.asList(tuple2("a", 2L), tuple2("b", 1L)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationSumDouble() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> agged = grouped.agg(typed.sum(v -> (double)v._2()));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3.0), tuple2("b", 3.0)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationSumLong() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> agged = grouped.agg(typed.sumLong(v -> (long)v._2()));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3L), tuple2("b", 3L)), agged.collectAsList());
  }
}
