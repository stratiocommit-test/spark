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

import org.apache.hive.service.cli.thrift.TOperationState;

/**
 * OperationState.
 *
 */
public enum OperationState {
  INITIALIZED(TOperationState.INITIALIZED_STATE, false),
  RUNNING(TOperationState.RUNNING_STATE, false),
  FINISHED(TOperationState.FINISHED_STATE, true),
  CANCELED(TOperationState.CANCELED_STATE, true),
  CLOSED(TOperationState.CLOSED_STATE, true),
  ERROR(TOperationState.ERROR_STATE, true),
  UNKNOWN(TOperationState.UKNOWN_STATE, false),
  PENDING(TOperationState.PENDING_STATE, false);

  private final TOperationState tOperationState;
  private final boolean terminal;

  OperationState(TOperationState tOperationState, boolean terminal) {
    this.tOperationState = tOperationState;
    this.terminal = terminal;
  }

  // must be sync with TOperationState in order
  public static OperationState getOperationState(TOperationState tOperationState) {
    return OperationState.values()[tOperationState.getValue()];
  }

  public static void validateTransition(OperationState oldState,
      OperationState newState)
          throws HiveSQLException {
    switch (oldState) {
    case INITIALIZED:
      switch (newState) {
      case PENDING:
      case RUNNING:
      case CANCELED:
      case CLOSED:
        return;
      }
      break;
    case PENDING:
      switch (newState) {
      case RUNNING:
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
        return;
      }
      break;
    case RUNNING:
      switch (newState) {
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
        return;
      }
      break;
    case FINISHED:
    case CANCELED:
    case ERROR:
      if (OperationState.CLOSED.equals(newState)) {
        return;
      }
      break;
    default:
      // fall-through
    }
    throw new HiveSQLException("Illegal Operation state transition " +
        "from " + oldState + " to " + newState);
  }

  public void validateTransition(OperationState newState)
      throws HiveSQLException {
    validateTransition(this, newState);
  }

  public TOperationState toTOperationState() {
    return tOperationState;
  }

  public boolean isTerminal() {
    return terminal;
  }
}
