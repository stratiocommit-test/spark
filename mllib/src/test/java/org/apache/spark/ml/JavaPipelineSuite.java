/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml;

import java.io.IOException;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.LogisticRegression;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Test Pipeline construction and fitting in Java.
 */
public class JavaPipelineSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    JavaRDD<LabeledPoint> points =
      jsc.parallelize(generateLogisticInputAsList(1.0, 1.0, 100, 42), 2);
    dataset = spark.createDataFrame(points, LabeledPoint.class);
  }

  @Test
  public void pipeline() {
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures");
    LogisticRegression lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures");
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[]{scaler, lr});
    PipelineModel model = pipeline.fit(dataset);
    model.transform(dataset).createOrReplaceTempView("prediction");
    Dataset<Row> predictions = spark.sql("SELECT label, probability, prediction FROM prediction");
    predictions.collectAsList();
  }
}
