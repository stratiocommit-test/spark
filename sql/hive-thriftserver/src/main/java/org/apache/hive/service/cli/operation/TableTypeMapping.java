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

import java.util.Set;


public interface TableTypeMapping {
  /**
   * Map client's table type name to hive's table type
   * @param clientTypeName
   * @return
   */
  String mapToHiveType(String clientTypeName);

  /**
   * Map hive's table type name to client's table type
   * @param clientTypeName
   * @return
   */
  String mapToClientType(String hiveTypeName);

  /**
   * Get all the table types of this mapping
   * @return
   */
  Set<String> getTableTypeNames();
}
