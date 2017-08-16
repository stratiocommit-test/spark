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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaPCASuite extends SharedSparkSession {

  public static class VectorPair implements Serializable {
    private Vector features = Vectors.dense(0.0);
    private Vector expected = Vectors.dense(0.0);

    public void setFeatures(Vector features) {
      this.features = features;
    }

    public Vector getFeatures() {
      return this.features;
    }

    public void setExpected(Vector expected) {
      this.expected = expected;
    }

    public Vector getExpected() {
      return this.expected;
    }
  }

  @Test
  public void testPCA() {
    List<Vector> points = Arrays.asList(
      Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0}),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    );
    JavaRDD<Vector> dataRDD = jsc.parallelize(points, 2);

    RowMatrix mat = new RowMatrix(dataRDD.map(
            new Function<Vector, org.apache.spark.mllib.linalg.Vector>() {
              public org.apache.spark.mllib.linalg.Vector call(Vector vector) {
                return new org.apache.spark.mllib.linalg.DenseVector(vector.toArray());
              }
            }
    ).rdd());

    Matrix pc = mat.computePrincipalComponents(3);

    mat.multiply(pc).rows().toJavaRDD();

    JavaRDD<Vector> expected = mat.multiply(pc).rows().toJavaRDD().map(
      new Function<org.apache.spark.mllib.linalg.Vector, Vector>() {
        public Vector call(org.apache.spark.mllib.linalg.Vector vector) {
          return vector.asML();
        }
      }
    );

    JavaRDD<VectorPair> featuresExpected = dataRDD.zip(expected).map(
      new Function<Tuple2<Vector, Vector>, VectorPair>() {
        public VectorPair call(Tuple2<Vector, Vector> pair) {
          VectorPair featuresExpected = new VectorPair();
          featuresExpected.setFeatures(pair._1());
          featuresExpected.setExpected(pair._2());
          return featuresExpected;
        }
      }
    );

    Dataset<Row> df = spark.createDataFrame(featuresExpected, VectorPair.class);
    PCAModel pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(3)
      .fit(df);
    List<Row> result = pca.transform(df).select("pca_features", "expected").toJavaRDD().collect();
    for (Row r : result) {
      Vector calculatedVector = (Vector) r.get(0);
      Vector expectedVector = (Vector) r.get(1);
      for (int i = 0; i < calculatedVector.size(); i++) {
        Assert.assertEquals(calculatedVector.apply(i), expectedVector.apply(i), 1.0e-8);
      }
    }
  }
}
