/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli.operation;

public class TableTypeMappingFactory {

  public enum TableTypeMappings {
    HIVE,
    CLASSIC
  }
  private static TableTypeMapping hiveTableTypeMapping = new HiveTableTypeMapping();
  private static TableTypeMapping classicTableTypeMapping = new ClassicTableTypeMapping();

  public static TableTypeMapping getTableTypeMapping(String mappingType) {
    if (TableTypeMappings.CLASSIC.toString().equalsIgnoreCase(mappingType)) {
      return classicTableTypeMapping;
    } else {
      return hiveTableTypeMapping;
    }
  }
}
