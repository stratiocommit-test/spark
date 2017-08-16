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

import org.apache.hive.service.cli.thrift.TFetchOrientation;

/**
 * FetchOrientation.
 *
 */
public enum FetchOrientation {
  FETCH_NEXT(TFetchOrientation.FETCH_NEXT),
  FETCH_PRIOR(TFetchOrientation.FETCH_PRIOR),
  FETCH_RELATIVE(TFetchOrientation.FETCH_RELATIVE),
  FETCH_ABSOLUTE(TFetchOrientation.FETCH_ABSOLUTE),
  FETCH_FIRST(TFetchOrientation.FETCH_FIRST),
  FETCH_LAST(TFetchOrientation.FETCH_LAST);

  private TFetchOrientation tFetchOrientation;

  FetchOrientation(TFetchOrientation tFetchOrientation) {
    this.tFetchOrientation = tFetchOrientation;
  }

  public static FetchOrientation getFetchOrientation(TFetchOrientation tFetchOrientation) {
    for (FetchOrientation fetchOrientation : values()) {
      if (tFetchOrientation.equals(fetchOrientation.toTFetchOrientation())) {
        return fetchOrientation;
      }
    }
    // TODO: Should this really default to FETCH_NEXT?
    return FETCH_NEXT;
  }

  public TFetchOrientation toTFetchOrientation() {
    return tFetchOrientation;
  }
}
