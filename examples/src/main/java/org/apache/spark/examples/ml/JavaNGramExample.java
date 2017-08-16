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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaNGramExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaNGramExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
      RowFactory.create(1, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
      RowFactory.create(2, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField(
        "words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
    });

    Dataset<Row> wordDataFrame = spark.createDataFrame(data, schema);

    NGram ngramTransformer = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");

    Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);
    ngramDataFrame.select("ngrams").show(false);
    // $example off$

    spark.stop();
  }
}
