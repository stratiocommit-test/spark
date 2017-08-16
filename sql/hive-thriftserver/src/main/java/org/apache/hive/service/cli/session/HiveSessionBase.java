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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import java.io.File;

/**
 * Methods that don't need to be executed under a doAs
 * context are here. Rest of them in HiveSession interface
 */
public interface HiveSessionBase {

  TProtocolVersion getProtocolVersion();

  /**
   * Set the session manager for the session
   * @param sessionManager
   */
  void setSessionManager(SessionManager sessionManager);

  /**
   * Get the session manager for the session
   */
  SessionManager getSessionManager();

  /**
   * Set operation manager for the session
   * @param operationManager
   */
  void setOperationManager(OperationManager operationManager);

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  boolean isOperationLogEnabled();

  /**
   * Get the session dir, which is the parent dir of operation logs
   * @return a file representing the parent directory of operation logs
   */
  File getOperationLogSessionDir();

  /**
   * Set the session dir, which is the parent dir of operation logs
   * @param operationLogRootDir the parent dir of the session dir
   */
  void setOperationLogSessionDir(File operationLogRootDir);

  SessionHandle getSessionHandle();

  String getUsername();

  String getPassword();

  HiveConf getHiveConf();

  SessionState getSessionState();

  String getUserName();

  void setUserName(String userName);

  String getIpAddress();

  void setIpAddress(String ipAddress);

  long getLastAccessTime();
}
