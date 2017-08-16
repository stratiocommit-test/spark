/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.ml;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaCountVectorizerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaCountVectorizerExample")
      .getOrCreate();

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("a", "b", "c")),
      RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
    );
    StructType schema = new StructType(new StructField [] {
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> df = spark.createDataFrame(data, schema);

    // fit a CountVectorizerModel from the corpus
    CountVectorizerModel cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("feature")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df);

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
      .setInputCol("text")
      .setOutputCol("feature");

    cvModel.transform(df).show(false);
    // $example off$

    spark.stop();
  }
}
