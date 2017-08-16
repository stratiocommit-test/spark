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

/**
 * FetchType indicates the type of fetchResults request.
 * It maps the TFetchType, which is generated from Thrift interface.
 */
public enum FetchType {
  QUERY_OUTPUT((short)0),
  LOG((short)1);

  private final short tFetchType;

  FetchType(short tFetchType) {
    this.tFetchType = tFetchType;
  }

  public static FetchType getFetchType(short tFetchType) {
    for (FetchType fetchType : values()) {
      if (tFetchType == fetchType.toTFetchType()) {
        return fetchType;
      }
    }
    return QUERY_OUTPUT;
  }

  public short toTFetchType() {
    return tFetchType;
  }
}
