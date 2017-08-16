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

import java.util.Arrays;
import java.util.List;

public class UDFToListInt extends UDF {
    public List<Integer> evaluate(Object o) {
        return Arrays.asList(1, 2, 3);
    }
}
