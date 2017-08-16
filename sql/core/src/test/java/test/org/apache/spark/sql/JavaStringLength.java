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

import org.apache.spark.sql.api.java.UDF1;

/**
 * It is used for register Java UDF from PySpark
 */
public class JavaStringLength implements UDF1<String, Integer> {
  @Override
  public Integer call(String str) throws Exception {
    return new Integer(str.length());
  }
}
