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

import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Map;

import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$

public class JavaVectorIndexerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaVectorIndexerExample")
      .getOrCreate();

    // $example on$
    Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    VectorIndexer indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10);
    VectorIndexerModel indexerModel = indexer.fit(data);

    Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
    System.out.print("Chose " + categoryMaps.size() + " categorical features:");

    for (Integer feature : categoryMaps.keySet()) {
      System.out.print(" " + feature);
    }
    System.out.println();

    // Create new column "indexed" with categorical values transformed to indices
    Dataset<Row> indexedData = indexerModel.transform(data);
    indexedData.show();
    // $example off$
    spark.stop();
  }
}
