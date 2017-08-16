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

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class JavaStopWordsRemoverSuite extends SharedSparkSession {

  @Test
  public void javaCompatibilityTest() {
    StopWordsRemover remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered");

    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
      RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false,
        Metadata.empty())
    });
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    remover.transform(dataset).collect();
  }
}
