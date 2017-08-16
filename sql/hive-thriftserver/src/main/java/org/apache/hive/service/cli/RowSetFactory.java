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

import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRowSet;

import static org.apache.hive.service.cli.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;

public class RowSetFactory {

  public static RowSet create(TableSchema schema, TProtocolVersion version) {
    if (version.getValue() >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
      return new ColumnBasedSet(schema);
    }
    return new RowBasedSet(schema);
  }

  public static RowSet create(TRowSet results, TProtocolVersion version) {
    if (version.getValue() >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
      return new ColumnBasedSet(results);
    }
    return new RowBasedSet(results);
  }
}
