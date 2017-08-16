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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence;
import org.apache.spark.util.Utils;

public class JavaPrefixSpanSuite extends SharedSparkSession {

  @Test
  public void runPrefixSpan() {
    JavaRDD<List<List<Integer>>> sequences = jsc.parallelize(Arrays.asList(
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
      Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
      Arrays.asList(Arrays.asList(6))
    ), 2);
    PrefixSpan prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5);
    PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
    JavaRDD<FreqSequence<Integer>> freqSeqs = model.freqSequences().toJavaRDD();
    List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
    Assert.assertEquals(5, localFreqSeqs.size());
    // Check that each frequent sequence could be materialized.
    for (PrefixSpan.FreqSequence<Integer> freqSeq : localFreqSeqs) {
      List<List<Integer>> seq = freqSeq.javaSequence();
      long freq = freqSeq.freq();
    }
  }

  @Test
  public void runPrefixSpanSaveLoad() {
    JavaRDD<List<List<Integer>>> sequences = jsc.parallelize(Arrays.asList(
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
      Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
      Arrays.asList(Arrays.asList(6))
    ), 2);
    PrefixSpan prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5);
    PrefixSpanModel<Integer> model = prefixSpan.run(sequences);

    File tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaPrefixSpanSuite");
    String outputPath = tempDir.getPath();

    try {
      model.save(spark.sparkContext(), outputPath);
      @SuppressWarnings("unchecked")
      PrefixSpanModel<Integer> newModel =
          (PrefixSpanModel<Integer>) PrefixSpanModel.load(spark.sparkContext(), outputPath);
      JavaRDD<FreqSequence<Integer>> freqSeqs = newModel.freqSequences().toJavaRDD();
      List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
      Assert.assertEquals(5, localFreqSeqs.size());
      // Check that each frequent sequence could be materialized.
      for (PrefixSpan.FreqSequence<Integer> freqSeq : localFreqSeqs) {
        List<List<Integer>> seq = freqSeq.javaSequence();
        long freq = freqSeq.freq();
      }
    } finally {
      Utils.deleteRecursively(tempDir);
    }


  }
}
