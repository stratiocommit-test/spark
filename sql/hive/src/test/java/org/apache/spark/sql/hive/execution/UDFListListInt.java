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

import java.util.List;

public class UDFListListInt extends UDF {
  /**
   * @param obj
   *   SQL schema: array&lt;struct&lt;x: int, y: int, z: int&gt;&gt;
   *   Java Type: List&lt;List&lt;Integer&gt;&gt;
   */
  @SuppressWarnings("unchecked")
  public long evaluate(Object obj) {
    if (obj == null) {
      return 0L;
    }
    List<List<?>> listList = (List<List<?>>) obj;
    long retVal = 0;
    for (List<?> aList : listList) {
      Number someInt = (Number) aList.get(1);
      try {
        retVal += someInt.longValue();
      } catch (NullPointerException e) {
        System.out.println(e);
      }
    }
    return retVal;
  }
}
