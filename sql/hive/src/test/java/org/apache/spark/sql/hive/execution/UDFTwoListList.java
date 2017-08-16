/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.execution;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFTwoListList extends UDF {
  public String evaluate(Object o1, Object o2) {
    UDFListListInt udf = new UDFListListInt();

    return String.format("%s, %s", udf.evaluate(o1), udf.evaluate(o2));
  }
}
