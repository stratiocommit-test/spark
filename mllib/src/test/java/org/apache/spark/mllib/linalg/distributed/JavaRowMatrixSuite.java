/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg.distributed;

import java.util.Arrays;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaRowMatrixSuite extends SharedSparkSession {

  @Test
  public void rowMatrixQRDecomposition() {
    Vector v1 = Vectors.dense(1.0, 10.0, 100.0);
    Vector v2 = Vectors.dense(2.0, 20.0, 200.0);
    Vector v3 = Vectors.dense(3.0, 30.0, 300.0);

    JavaRDD<Vector> rows = jsc.parallelize(Arrays.asList(v1, v2, v3), 1);
    RowMatrix mat = new RowMatrix(rows.rdd());

    QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
  }
}
