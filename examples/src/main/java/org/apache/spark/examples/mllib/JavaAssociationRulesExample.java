/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.mllib;

// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
// $example off$

import org.apache.spark.SparkConf;

public class JavaAssociationRulesExample {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("JavaAssociationRulesExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // $example on$
    JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = sc.parallelize(Arrays.asList(
      new FreqItemset<String>(new String[] {"a"}, 15L),
      new FreqItemset<String>(new String[] {"b"}, 35L),
      new FreqItemset<String>(new String[] {"a", "b"}, 12L)
    ));

    AssociationRules arules = new AssociationRules()
      .setMinConfidence(0.8);
    JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);

    for (AssociationRules.Rule<String> rule : results.collect()) {
      System.out.println(
        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    }
    // $example off$

    sc.stop();
  }
}
