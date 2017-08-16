/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli.session;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.*;

public interface HiveSession extends HiveSessionBase {

  void open(Map<String, String> sessionConfMap) throws Exception;

  IMetaStoreClient getMetaStoreClient() throws HiveSQLException;

  /**
   * getInfo operation handler
   * @param getInfoType
   * @return
   * @throws HiveSQLException
   */
  GetInfoValue getInfo(GetInfoType getInfoType) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatement(String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  OperationHandle executeStatementAsync(String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /**
   * getTypeInfo operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTypeInfo() throws HiveSQLException;

  /**
   * getCatalogs operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getCatalogs() throws HiveSQLException;

  /**
   * getSchemas operation handler
   * @param catalogName
   * @param schemaName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException;

  /**
   * getTables operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param tableTypes
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTables(String catalogName, String schemaName,
      String tableName, List<String> tableTypes) throws HiveSQLException;

  /**
   * getTableTypes operation handler
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getTableTypes() throws HiveSQLException ;

  /**
   * getColumns operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param columnName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException;

  /**
   * getFunctions operation handler
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws HiveSQLException
   */
  OperationHandle getFunctions(String catalogName, String schemaName,
      String functionName) throws HiveSQLException;

  /**
   * close the session
   * @throws HiveSQLException
   */
  void close() throws HiveSQLException;

  void cancelOperation(OperationHandle opHandle) throws HiveSQLException;

  void closeOperation(OperationHandle opHandle) throws HiveSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException;

  RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException;

  String getDelegationToken(HiveAuthFactory authFactory, String owner,
      String renewer) throws HiveSQLException;

  void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;

  void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException;

  void closeExpiredOperations();

  long getNoOperationTime();
}
