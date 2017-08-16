/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

import org.apache.parquet.io.api.Binary;

/**
 * Interface for value decoding that supports vectorized (aka batched) decoding.
 * TODO: merge this into parquet-mr.
 */
public interface VectorizedValuesReader {
  boolean readBoolean();
  byte readByte();
  int readInteger();
  long readLong();
  float readFloat();
  double readDouble();
  Binary readBinary(int len);

  /*
   * Reads `total` values into `c` start at `c[rowId]`
   */
  void readBooleans(int total, ColumnVector c, int rowId);
  void readBytes(int total, ColumnVector c, int rowId);
  void readIntegers(int total, ColumnVector c, int rowId);
  void readLongs(int total, ColumnVector c, int rowId);
  void readFloats(int total, ColumnVector c, int rowId);
  void readDoubles(int total, ColumnVector c, int rowId);
  void readBinary(int total, ColumnVector c, int rowId);
}
