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

import com.google.common.base.Strings;

import scala.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;

public class JavaWord2VecSuite extends SharedSparkSession {

  @Test
  @SuppressWarnings("unchecked")
  public void word2Vec() {
    // The tests are to check Java compatibility.
    String sentence = Strings.repeat("a b ", 100) + Strings.repeat("a c ", 10);
    List<String> words = Arrays.asList(sentence.split(" "));
    List<List<String>> localDoc = Arrays.asList(words, words);
    JavaRDD<List<String>> doc = jsc.parallelize(localDoc);
    Word2Vec word2vec = new Word2Vec()
      .setVectorSize(10)
      .setSeed(42L);
    Word2VecModel model = word2vec.fit(doc);
    Tuple2<String, Object>[] syms = model.findSynonyms("a", 2);
    Assert.assertEquals(2, syms.length);
    Assert.assertEquals("b", syms[0]._1());
    Assert.assertEquals("c", syms[1]._1());
  }
}
