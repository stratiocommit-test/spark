/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.ml;

import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaBucketedRandomProjectionLSHExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaBucketedRandomProjectionLSHExample")
      .getOrCreate();

    // $example on$
    List<Row> dataA = Arrays.asList(
      RowFactory.create(0, Vectors.dense(1.0, 1.0)),
      RowFactory.create(1, Vectors.dense(1.0, -1.0)),
      RowFactory.create(2, Vectors.dense(-1.0, -1.0)),
      RowFactory.create(3, Vectors.dense(-1.0, 1.0))
    );

    List<Row> dataB = Arrays.asList(
        RowFactory.create(4, Vectors.dense(1.0, 0.0)),
        RowFactory.create(5, Vectors.dense(-1.0, 0.0)),
        RowFactory.create(6, Vectors.dense(0.0, 1.0)),
        RowFactory.create(7, Vectors.dense(0.0, -1.0))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("keys", new VectorUDT(), false, Metadata.empty())
    });
    Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
    Dataset<Row> dfB = spark.createDataFrame(dataB, schema);

    Vector key = Vectors.dense(1.0, 0.0);

    BucketedRandomProjectionLSH mh = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values");

    BucketedRandomProjectionLSHModel model = mh.fit(dfA);

    // Feature Transformation
    model.transform(dfA).show();
    // Cache the transformed columns
    Dataset<Row> transformedA = model.transform(dfA).cache();
    Dataset<Row> transformedB = model.transform(dfB).cache();

    // Approximate similarity join
    model.approxSimilarityJoin(dfA, dfB, 1.5).show();
    model.approxSimilarityJoin(transformedA, transformedB, 1.5).show();
    // Self Join
    model.approxSimilarityJoin(dfA, dfA, 2.5).filter("datasetA.id < datasetB.id").show();

    // Approximate nearest neighbor search
    model.approxNearestNeighbors(dfA, key, 2).show();
    model.approxNearestNeighbors(transformedA, key, 2).show();
    // $example off$

    spark.stop();
  }
}
