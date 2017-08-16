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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaPolynomialExpansionSuite extends SharedSparkSession {

  @Test
  public void polynomialExpansionTest() {
    PolynomialExpansion polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3);

    List<Row> data = Arrays.asList(
      RowFactory.create(
        Vectors.dense(-2.0, 2.3),
        Vectors.dense(-2.0, 4.0, -8.0, 2.3, -4.6, 9.2, 5.29, -10.58, 12.17)
      ),
      RowFactory.create(Vectors.dense(0.0, 0.0), Vectors.dense(new double[9])),
      RowFactory.create(
        Vectors.dense(0.6, -1.1),
        Vectors.dense(0.6, 0.36, 0.216, -1.1, -0.66, -0.396, 1.21, 0.726, -1.331)
      )
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("expected", new VectorUDT(), false, Metadata.empty())
    });

    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    List<Row> pairs = polyExpansion.transform(dataset)
      .select("polyFeatures", "expected")
      .collectAsList();

    for (Row r : pairs) {
      double[] polyFeatures = ((Vector) r.get(0)).toArray();
      double[] expected = ((Vector) r.get(1)).toArray();
      Assert.assertArrayEquals(polyFeatures, expected, 1e-1);
    }
  }
}
