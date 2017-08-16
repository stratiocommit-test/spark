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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.feature.VectorIndexerSuite.FeatureData;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class JavaVectorIndexerSuite extends SharedSparkSession {

  @Test
  public void vectorIndexerAPI() {
    // The tests are to check Java compatibility.
    List<FeatureData> points = Arrays.asList(
      new FeatureData(Vectors.dense(0.0, -2.0)),
      new FeatureData(Vectors.dense(1.0, 3.0)),
      new FeatureData(Vectors.dense(1.0, 4.0))
    );
    Dataset<Row> data = spark.createDataFrame(jsc.parallelize(points, 2), FeatureData.class);
    VectorIndexer indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(2);
    VectorIndexerModel model = indexer.fit(data);
    Assert.assertEquals(model.numFeatures(), 2);
    Map<Integer, Map<Double, Integer>> categoryMaps = model.javaCategoryMaps();
    Assert.assertEquals(categoryMaps.size(), 1);
    Dataset<Row> indexedData = model.transform(data);
  }
}
