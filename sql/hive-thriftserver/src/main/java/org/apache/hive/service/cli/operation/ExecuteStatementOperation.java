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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;
  protected Map<String, String> confOverlay = new HashMap<String, String>();

  public ExecuteStatementOperation(HiveSession parentSession, String statement,
      Map<String, String> confOverlay, boolean runInBackground) {
    super(parentSession, OperationType.EXECUTE_STATEMENT, runInBackground);
    this.statement = statement;
    setConfOverlay(confOverlay);
  }

  public String getStatement() {
    return statement;
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(
      HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runAsync)
          throws HiveSQLException {
    String[] tokens = statement.trim().split("\\s+");
    CommandProcessor processor = null;
    try {
      processor = CommandProcessorFactory.getForHiveCommand(tokens, parentSession.getHiveConf());
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getSQLState(), e);
    }
    if (processor == null) {
      return new SQLOperation(parentSession, statement, confOverlay, runAsync);
    }
    return new HiveCommandOperation(parentSession, statement, processor, confOverlay);
  }

  protected Map<String, String> getConfOverlay() {
    return confOverlay;
  }

  protected void setConfOverlay(Map<String, String> confOverlay) {
    if (confOverlay != null) {
      this.confOverlay = confOverlay;
    }
  }
}
