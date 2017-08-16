/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.clustering;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaKMeansSuite extends SharedSparkSession {

  private transient int k = 5;
  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k);
  }

  @Test
  public void fitAndTransform() {
    KMeans kmeans = new KMeans().setK(k).setSeed(1);
    KMeansModel model = kmeans.fit(dataset);

    Vector[] centers = model.clusterCenters();
    assertEquals(k, centers.length);

    Dataset<Row> transformed = model.transform(dataset);
    List<String> columns = Arrays.asList(transformed.columns());
    List<String> expectedColumns = Arrays.asList("features", "prediction");
    for (String column : expectedColumns) {
      assertTrue(columns.contains(column));
    }
  }
}
