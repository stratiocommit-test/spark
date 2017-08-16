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

import org.apache.hive.service.cli.thrift.TOperationType;

/**
 * OperationType.
 *
 */
public enum OperationType {

  UNKNOWN_OPERATION(TOperationType.UNKNOWN),
  EXECUTE_STATEMENT(TOperationType.EXECUTE_STATEMENT),
  GET_TYPE_INFO(TOperationType.GET_TYPE_INFO),
  GET_CATALOGS(TOperationType.GET_CATALOGS),
  GET_SCHEMAS(TOperationType.GET_SCHEMAS),
  GET_TABLES(TOperationType.GET_TABLES),
  GET_TABLE_TYPES(TOperationType.GET_TABLE_TYPES),
  GET_COLUMNS(TOperationType.GET_COLUMNS),
  GET_FUNCTIONS(TOperationType.GET_FUNCTIONS);

  private TOperationType tOperationType;

  OperationType(TOperationType tOpType) {
    this.tOperationType = tOpType;
  }

  public static OperationType getOperationType(TOperationType tOperationType) {
    // TODO: replace this with a Map?
    for (OperationType opType : values()) {
      if (tOperationType.equals(opType.tOperationType)) {
        return opType;
      }
    }
    return OperationType.UNKNOWN_OPERATION;
  }

  public TOperationType toTOperationType() {
    return tOperationType;
  }
}
