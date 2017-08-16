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

import java.util.Arrays;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;

public class JavaAssociationRulesSuite extends SharedSparkSession {

  @Test
  public void runAssociationRules() {

    @SuppressWarnings("unchecked")
    JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = jsc.parallelize(Arrays.asList(
      new FreqItemset<String>(new String[]{"a"}, 15L),
      new FreqItemset<String>(new String[]{"b"}, 35L),
      new FreqItemset<String>(new String[]{"a", "b"}, 12L)
    ));

    JavaRDD<AssociationRules.Rule<String>> results = (new AssociationRules()).run(freqItemsets);
  }
}
