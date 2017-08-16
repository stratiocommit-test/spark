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

import java.util.Collections;

import org.apache.hive.service.auth.HiveAuthFactory;


/**
 * CLIServiceClient.
 *
 */
public abstract class CLIServiceClient implements ICLIService {
  private static final long DEFAULT_MAX_ROWS = 1000;

  public SessionHandle openSession(String username, String password)
      throws HiveSQLException {
    return openSession(username, password, Collections.<String, String>emptyMap());
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    // TODO: provide STATIC default value
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, DEFAULT_MAX_ROWS, FetchType.QUERY_OUTPUT);
  }

  @Override
  public abstract String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException;

  @Override
  public abstract void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

  @Override
  public abstract void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

}
