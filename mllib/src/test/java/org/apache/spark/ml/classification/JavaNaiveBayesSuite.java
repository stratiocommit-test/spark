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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaNaiveBayesSuite extends SharedSparkSession {

  public void validatePrediction(Dataset<Row> predictionAndLabels) {
    for (Row r : predictionAndLabels.collectAsList()) {
      double prediction = r.getAs(0);
      double label = r.getAs(1);
      assertEquals(label, prediction, 1E-5);
    }
  }

  @Test
  public void naiveBayesDefaultParams() {
    NaiveBayes nb = new NaiveBayes();
    assertEquals("label", nb.getLabelCol());
    assertEquals("features", nb.getFeaturesCol());
    assertEquals("prediction", nb.getPredictionCol());
    assertEquals(1.0, nb.getSmoothing(), 1E-5);
    assertEquals("multinomial", nb.getModelType());
  }

  @Test
  public void testNaiveBayes() {
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      RowFactory.create(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      RowFactory.create(1.0, Vectors.dense(0.0, 1.0, 0.0)),
      RowFactory.create(1.0, Vectors.dense(0.0, 2.0, 0.0)),
      RowFactory.create(2.0, Vectors.dense(0.0, 0.0, 1.0)),
      RowFactory.create(2.0, Vectors.dense(0.0, 0.0, 2.0)));

    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty())
    });

    Dataset<Row> dataset = spark.createDataFrame(data, schema);
    NaiveBayes nb = new NaiveBayes().setSmoothing(0.5).setModelType("multinomial");
    NaiveBayesModel model = nb.fit(dataset);

    Dataset<Row> predictionAndLabels = model.transform(dataset).select("prediction", "label");
    validatePrediction(predictionAndLabels);
  }
}
