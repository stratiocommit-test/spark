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

import static org.apache.spark.sql.types.DataTypes.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaVectorAssemblerSuite extends SharedSparkSession {

  @Test
  public void testVectorAssembler() {
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("x", DoubleType, false),
      createStructField("y", new VectorUDT(), false),
      createStructField("name", StringType, false),
      createStructField("z", new VectorUDT(), false),
      createStructField("n", LongType, false)
    });
    Row row = RowFactory.create(
      0, 0.0, Vectors.dense(1.0, 2.0), "a",
      Vectors.sparse(2, new int[]{1}, new double[]{3.0}), 10L);
    Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row), schema);
    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[]{"x", "y", "z", "n"})
      .setOutputCol("features");
    Dataset<Row> output = assembler.transform(dataset);
    Assert.assertEquals(
      Vectors.sparse(6, new int[]{1, 2, 4, 5}, new double[]{1.0, 2.0, 3.0, 10.0}),
      output.select("features").first().<Vector>getAs(0));
  }
}
