/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.java.function;

import java.io.Serializable;

/**
 * A three-argument function that takes arguments of type T1, T2 and T3 and returns an R.
 */
public interface Function3<T1, T2, T3, R> extends Serializable {
  R call(T1 v1, T2 v2, T3 v3) throws Exception;
}
