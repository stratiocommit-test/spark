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

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaStandardScalerSuite extends SharedSparkSession {

  @Test
  public void standardScaler() {
    // The tests are to check Java compatibility.
    List<VectorIndexerSuite.FeatureData> points = Arrays.asList(
      new VectorIndexerSuite.FeatureData(Vectors.dense(0.0, -2.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 3.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 4.0))
    );
    Dataset<Row> dataFrame = spark.createDataFrame(jsc.parallelize(points, 2),
      VectorIndexerSuite.FeatureData.class);
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false);

    // Compute summary statistics by fitting the StandardScaler
    StandardScalerModel scalerModel = scaler.fit(dataFrame);

    // Normalize each feature to have unit standard deviation.
    Dataset<Row> scaledData = scalerModel.transform(dataFrame);
    scaledData.count();
  }
}
