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

import static org.apache.spark.sql.types.DataTypes.*;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaStringIndexerSuite extends SharedSparkSession {

  @Test
  public void testStringIndexer() {
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("label", StringType, false)
    });
    List<Row> data = Arrays.asList(
      cr(0, "a"), cr(1, "b"), cr(2, "c"), cr(3, "a"), cr(4, "a"), cr(5, "c"));
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    StringIndexer indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex");
    Dataset<Row> output = indexer.fit(dataset).transform(dataset);

    Assert.assertEquals(
      Arrays.asList(cr(0, 0.0), cr(1, 2.0), cr(2, 1.0), cr(3, 0.0), cr(4, 0.0), cr(5, 1.0)),
      output.orderBy("id").select("id", "labelIndex").collectAsList());
  }

  /**
   * An alias for RowFactory.create.
   */
  private Row cr(Object... values) {
    return RowFactory.create(values);
  }
}
