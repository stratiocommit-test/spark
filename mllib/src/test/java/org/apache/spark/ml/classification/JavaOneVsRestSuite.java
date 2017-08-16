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

import java.io.IOException;
import java.util.List;

import scala.collection.JavaConverters;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateMultinomialLogisticInput;

public class JavaOneVsRestSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;
  private transient JavaRDD<LabeledPoint> datasetRDD;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    int nPoints = 3;

    // The following coefficients and xMean/xVariance are computed from iris dataset with
    // lambda=0.2.
    // As a result, we are drawing samples from probability distribution of an actual model.
    double[] coefficients = {
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682};

    double[] xMean = {5.843, 3.057, 3.758, 1.199};
    double[] xVariance = {0.6856, 0.1899, 3.116, 0.581};
    List<LabeledPoint> points = JavaConverters.seqAsJavaListConverter(
      generateMultinomialLogisticInput(coefficients, xMean, xVariance, true, nPoints, 42)
    ).asJava();
    datasetRDD = jsc.parallelize(points, 2);
    dataset = spark.createDataFrame(datasetRDD, LabeledPoint.class);
  }

  @Test
  public void oneVsRestDefaultParams() {
    OneVsRest ova = new OneVsRest();
    ova.setClassifier(new LogisticRegression());
    Assert.assertEquals(ova.getLabelCol(), "label");
    Assert.assertEquals(ova.getPredictionCol(), "prediction");
    OneVsRestModel ovaModel = ova.fit(dataset);
    Dataset<Row> predictions = ovaModel.transform(dataset).select("label", "prediction");
    predictions.collectAsList();
    Assert.assertEquals(ovaModel.getLabelCol(), "label");
    Assert.assertEquals(ovaModel.getPredictionCol(), "prediction");
  }
}
