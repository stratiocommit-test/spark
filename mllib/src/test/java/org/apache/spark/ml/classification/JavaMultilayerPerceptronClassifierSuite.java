/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.classification;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaMultilayerPerceptronClassifierSuite extends SharedSparkSession {

  @Test
  public void testMLPC() {
    List<LabeledPoint> data = Arrays.asList(
      new LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      new LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
      new LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      new LabeledPoint(0.0, Vectors.dense(1.0, 1.0))
    );
    Dataset<Row> dataFrame = spark.createDataFrame(data, LabeledPoint.class);

    MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
      .setLayers(new int[]{2, 5, 2})
      .setBlockSize(1)
      .setSeed(123L)
      .setMaxIter(100);
    MultilayerPerceptronClassificationModel model = mlpc.fit(dataFrame);
    Dataset<Row> result = model.transform(dataFrame);
    List<Row> predictionAndLabels = result.select("prediction", "label").collectAsList();
    for (Row r : predictionAndLabels) {
      Assert.assertEquals((int) r.getDouble(0), (int) r.getDouble(1));
    }
  }
}
