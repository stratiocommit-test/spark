/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.util.Utils;

public class JavaMetastoreDataSourcesSuite {
  private transient JavaSparkContext sc;
  private transient SQLContext sqlContext;

  File path;
  Path hiveManagedPath;
  FileSystem fs;
  Dataset<Row> df;

  private static void checkAnswer(Dataset<Row> actual, List<Row> expected) {
    String errorMessage = QueryTest$.MODULE$.checkAnswer(actual, expected);
    if (errorMessage != null) {
      Assert.fail(errorMessage);
    }
  }

  @Before
  public void setUp() throws IOException {
    sqlContext = TestHive$.MODULE$;
    sc = new JavaSparkContext(sqlContext.sparkContext());

    path =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }
    HiveSessionCatalog catalog = (HiveSessionCatalog) sqlContext.sessionState().catalog();
    hiveManagedPath = new Path(
      catalog.hiveDefaultTableFilePath(new TableIdentifier("javaSavedTable")));
    fs = hiveManagedPath.getFileSystem(sc.hadoopConfiguration());
    fs.delete(hiveManagedPath, true);

    List<String> jsonObjects = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"a\":" + i + ", \"b\":\"str" + i + "\"}");
    }
    JavaRDD<String> rdd = sc.parallelize(jsonObjects);
    df = sqlContext.read().json(rdd);
    df.createOrReplaceTempView("jsonTable");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up tables.
    if (sqlContext != null) {
      sqlContext.sql("DROP TABLE IF EXISTS javaSavedTable");
      sqlContext.sql("DROP TABLE IF EXISTS externalTable");
    }
  }

  @Test
  public void saveExternalTableAndQueryIt() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write()
      .format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .options(options)
      .saveAsTable("javaSavedTable");

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());

    Dataset<Row> loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", options);

    checkAnswer(loadedDF, df.collectAsList());
    checkAnswer(
      sqlContext.sql("SELECT * FROM externalTable"),
      df.collectAsList());
  }

  @Test
  public void saveExternalTableWithSchemaAndQueryIt() {
    Map<String, String> options = new HashMap<>();
    options.put("path", path.toString());
    df.write()
      .format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .options(options)
      .saveAsTable("javaSavedTable");

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("b", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    Dataset<Row> loadedDF =
      sqlContext.createExternalTable("externalTable", "org.apache.spark.sql.json", schema, options);

    checkAnswer(
      loadedDF,
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList());
    checkAnswer(
      sqlContext.sql("SELECT * FROM externalTable"),
      sqlContext.sql("SELECT b FROM javaSavedTable").collectAsList());
  }

  @Test
  public void saveTableAndQueryIt() {
    Map<String, String> options = new HashMap<>();
    df.write()
      .format("org.apache.spark.sql.json")
      .mode(SaveMode.Append)
      .options(options)
      .saveAsTable("javaSavedTable");

    checkAnswer(
      sqlContext.sql("SELECT * FROM javaSavedTable"),
      df.collectAsList());
  }
}
