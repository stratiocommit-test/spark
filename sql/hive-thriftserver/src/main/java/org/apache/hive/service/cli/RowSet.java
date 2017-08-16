/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli;

import org.apache.hive.service.cli.thrift.TRowSet;

public interface RowSet extends Iterable<Object[]> {

  RowSet addRow(Object[] fields);

  RowSet extractSubset(int maxRows);

  int numColumns();

  int numRows();

  long getStartOffset();

  void setStartOffset(long startOffset);

  TRowSet toTRowSet();
}
