/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.api.java;

import java.io.Serializable;

import org.apache.spark.annotation.InterfaceStability;

/**
 * A Spark SQL UDF that has 2 arguments.
 */
@InterfaceStability.Stable
public interface UDF2<T1, T2, R> extends Serializable {
  R call(T1 t1, T2 t2) throws Exception;
}
