/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaTokenizerSuite extends SharedSparkSession {

  @Test
  public void regexTokenizer() {
    RegexTokenizer myRegExTokenizer = new RegexTokenizer()
      .setInputCol("rawText")
      .setOutputCol("tokens")
      .setPattern("\\s")
      .setGaps(true)
      .setToLowercase(false)
      .setMinTokenLength(3);


    JavaRDD<TokenizerTestData> rdd = jsc.parallelize(Arrays.asList(
      new TokenizerTestData("Test of tok.", new String[]{"Test", "tok."}),
      new TokenizerTestData("Te,st.  punct", new String[]{"Te,st.", "punct"})
    ));
    Dataset<Row> dataset = spark.createDataFrame(rdd, TokenizerTestData.class);

    List<Row> pairs = myRegExTokenizer.transform(dataset)
      .select("tokens", "wantedTokens")
      .collectAsList();

    for (Row r : pairs) {
      Assert.assertEquals(r.get(0), r.get(1));
    }
  }
}
