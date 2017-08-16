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

import java.io.Serializable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaUDFSuite implements Serializable {
  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf1Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest", (String str) -> str.length(), DataType.IntegerType);

    spark.udf().register("stringLengthTest", new UDF1<String, Integer>() {
      @Override
      public Integer call(String str) {
        return str.length();
      }
    }, DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test')").head();
    Assert.assertEquals(4, result.getInt(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf2Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest",
    //   (String str1, String str2) -> str1.length() + str2.length,
    //   DataType.IntegerType);

    spark.udf().register("stringLengthTest", new UDF2<String, String, Integer>() {
      @Override
      public Integer call(String str1, String str2) {
        return str1.length() + str2.length();
      }
    }, DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));
  }

  public static class StringLengthTest implements UDF2<String, String, Integer> {
    @Override
    public Integer call(String str1, String str2) throws Exception {
      return new Integer(str1.length() + str2.length());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf3Test() {
    spark.udf().registerJava("stringLengthTest", StringLengthTest.class.getName(),
        DataTypes.IntegerType);
    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));

    // returnType is not provided
    spark.udf().registerJava("stringLengthTest2", StringLengthTest.class.getName(), null);
    result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));
  }
}
