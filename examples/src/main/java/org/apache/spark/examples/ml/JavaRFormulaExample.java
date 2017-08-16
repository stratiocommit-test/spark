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

import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;
// $example off$

public class JavaRFormulaExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaRFormulaExample")
      .getOrCreate();

    // $example on$
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("country", StringType, false),
      createStructField("hour", IntegerType, false),
      createStructField("clicked", DoubleType, false)
    });

    List<Row> data = Arrays.asList(
      RowFactory.create(7, "US", 18, 1.0),
      RowFactory.create(8, "CA", 12, 0.0),
      RowFactory.create(9, "NZ", 15, 0.0)
    );

    Dataset<Row> dataset = spark.createDataFrame(data, schema);
    RFormula formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label");
    Dataset<Row> output = formula.fit(dataset).transform(dataset);
    output.select("features", "label").show();
    // $example off$
    spark.stop();
  }
}

