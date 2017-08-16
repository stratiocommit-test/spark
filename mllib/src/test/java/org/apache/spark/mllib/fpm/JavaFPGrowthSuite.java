/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.fpm;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.Utils;

public class JavaFPGrowthSuite extends SharedSparkSession {

  @Test
  public void runFPGrowth() {

    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> rdd = jsc.parallelize(Arrays.asList(
      Arrays.asList("r z h k p".split(" ")),
      Arrays.asList("z y x w v u t s".split(" ")),
      Arrays.asList("s x o n r".split(" ")),
      Arrays.asList("x z y m t s q e".split(" ")),
      Arrays.asList("z".split(" ")),
      Arrays.asList("x z y r q t p".split(" "))), 2);

    FPGrowthModel<String> model = new FPGrowth()
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd);

    List<FPGrowth.FreqItemset<String>> freqItemsets = model.freqItemsets().toJavaRDD().collect();
    assertEquals(18, freqItemsets.size());

    for (FPGrowth.FreqItemset<String> itemset : freqItemsets) {
      // Test return types.
      List<String> items = itemset.javaItems();
      long freq = itemset.freq();
    }
  }

  @Test
  public void runFPGrowthSaveLoad() {

    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> rdd = jsc.parallelize(Arrays.asList(
      Arrays.asList("r z h k p".split(" ")),
      Arrays.asList("z y x w v u t s".split(" ")),
      Arrays.asList("s x o n r".split(" ")),
      Arrays.asList("x z y m t s q e".split(" ")),
      Arrays.asList("z".split(" ")),
      Arrays.asList("x z y r q t p".split(" "))), 2);

    FPGrowthModel<String> model = new FPGrowth()
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd);

    File tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaFPGrowthSuite");
    String outputPath = tempDir.getPath();

    try {
      model.save(spark.sparkContext(), outputPath);
      @SuppressWarnings("unchecked")
      FPGrowthModel<String> newModel =
        (FPGrowthModel<String>) FPGrowthModel.load(spark.sparkContext(), outputPath);
      List<FPGrowth.FreqItemset<String>> freqItemsets = newModel.freqItemsets().toJavaRDD()
        .collect();
      assertEquals(18, freqItemsets.size());

      for (FPGrowth.FreqItemset<String> itemset : freqItemsets) {
        // Test return types.
        List<String> items = itemset.javaItems();
        long freq = itemset.freq();
      }
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
