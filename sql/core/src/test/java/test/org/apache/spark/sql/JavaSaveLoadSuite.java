/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package test.org.apache.spark.sql;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

public class JavaSaveLoadSuite {

  private transient SparkSession spark;
  private transient JavaSparkContext jsc;

  File path;
  Dataset<Row> df;

  private static void checkAnswer(Dataset<Row> actual, List<Row> expected) {
    String errorMessage = QueryTest$.MODULE$.checkAnswer(actual, expected);
    if (errorMessage != null) {
      Assert.fail(errorMessage);
    }
  }

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());

    path =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }

    List<String> jsonObjects = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    JavaRDD<String> rdd = jsc.parallelize(jsonObjects);
    df = spark.read().json(rdd);
    df.createOrReplaceTempView("jsonTable");
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void saveAndLoad() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write().mode(SaveMode.ErrorIfExists).format("json").options(options).save();
    Dataset<Row> loadedDF = spark.read().format("json").options(options).load();
    checkAnswer(loadedDF, df.collectAsList());
  }

  @Test
  public void saveAndLoadWithSchema() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write().format("json").mode(SaveMode.ErrorIfExists).options(options).save();

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    Dataset<Row> loadedDF = spark.read().format("json").schema(schema).options(options).load();

    checkAnswer(loadedDF, spark.sql("SELECT b FROM jsonTable").collectAsList());
  }
}
