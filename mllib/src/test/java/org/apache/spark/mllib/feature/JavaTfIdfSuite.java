/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.feature;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

public class JavaTfIdfSuite extends SharedSparkSession {

  @Test
  public void tfIdf() {
    // The tests are to check Java compatibility.
    HashingTF tf = new HashingTF();
    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> documents = jsc.parallelize(Arrays.asList(
      Arrays.asList("this is a sentence".split(" ")),
      Arrays.asList("this is another sentence".split(" ")),
      Arrays.asList("this is still a sentence".split(" "))), 2);
    JavaRDD<Vector> termFreqs = tf.transform(documents);
    termFreqs.collect();
    IDF idf = new IDF();
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    List<Vector> localTfIdfs = tfIdfs.collect();
    int indexOfThis = tf.indexOf("this");
    for (Vector v : localTfIdfs) {
      Assert.assertEquals(0.0, v.apply(indexOfThis), 1e-15);
    }
  }

  @Test
  public void tfIdfMinimumDocumentFrequency() {
    // The tests are to check Java compatibility.
    HashingTF tf = new HashingTF();
    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> documents = jsc.parallelize(Arrays.asList(
      Arrays.asList("this is a sentence".split(" ")),
      Arrays.asList("this is another sentence".split(" ")),
      Arrays.asList("this is still a sentence".split(" "))), 2);
    JavaRDD<Vector> termFreqs = tf.transform(documents);
    termFreqs.collect();
    IDF idf = new IDF(2);
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    List<Vector> localTfIdfs = tfIdfs.collect();
    int indexOfThis = tf.indexOf("this");
    for (Vector v : localTfIdfs) {
      Assert.assertEquals(0.0, v.apply(indexOfThis), 1e-15);
    }
  }

}
