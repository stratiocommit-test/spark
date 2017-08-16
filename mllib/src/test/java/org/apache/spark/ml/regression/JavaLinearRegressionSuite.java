/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.regression;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaLinearRegressionSuite extends SharedSparkSession {
  private transient Dataset<Row> dataset;
  private transient JavaRDD<LabeledPoint> datasetRDD;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    datasetRDD = jsc.parallelize(points, 2);
    dataset = spark.createDataFrame(datasetRDD, LabeledPoint.class);
    dataset.createOrReplaceTempView("dataset");
  }

  @Test
  public void linearRegressionDefaultParams() {
    LinearRegression lr = new LinearRegression();
    assertEquals("label", lr.getLabelCol());
    assertEquals("auto", lr.getSolver());
    LinearRegressionModel model = lr.fit(dataset);
    model.transform(dataset).createOrReplaceTempView("prediction");
    Dataset<Row> predictions = spark.sql("SELECT label, prediction FROM prediction");
    predictions.collect();
    // Check defaults
    assertEquals("features", model.getFeaturesCol());
    assertEquals("prediction", model.getPredictionCol());
  }

  @Test
  public void linearRegressionWithSetters() {
    // Set params, train, and check as many params as we can.
    LinearRegression lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(1.0).setSolver("l-bfgs");
    LinearRegressionModel model = lr.fit(dataset);
    LinearRegression parent = (LinearRegression) model.parent();
    assertEquals(10, parent.getMaxIter());
    assertEquals(1.0, parent.getRegParam(), 0.0);

    // Call fit() with new params, and check as many params as we can.
    LinearRegressionModel model2 =
      lr.fit(dataset, lr.maxIter().w(5), lr.regParam().w(0.1), lr.predictionCol().w("thePred"));
    LinearRegression parent2 = (LinearRegression) model2.parent();
    assertEquals(5, parent2.getMaxIter());
    assertEquals(0.1, parent2.getRegParam(), 0.0);
    assertEquals("thePred", model2.getPredictionCol());
  }
}
