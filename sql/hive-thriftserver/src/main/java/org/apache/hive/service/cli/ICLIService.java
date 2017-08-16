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

import java.util.List;
import java.util.Map;




import org.apache.hive.service.auth.HiveAuthFactory;

public interface ICLIService {

  SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws HiveSQLException;

  SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken)
          throws HiveSQLException;

  void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException;

  GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws HiveSQLException;

  OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay)
          throws HiveSQLException;

  OperationHandle executeStatementAsync(SessionHandle sessionHandle,
      String statement, Map<String, String> confOverlay)
          throws HiveSQLException;

  OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
          throws HiveSQLException;

  OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
          throws HiveSQLException;

  OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException;

  OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws HiveSQLException;

  OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
          throws HiveSQLException;

  OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException;

  void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException;

  void closeOperation(OperationHandle opHandle)
      throws HiveSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException;

  RowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException;

  RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException;

  String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException;

  void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;

  void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException;


}
