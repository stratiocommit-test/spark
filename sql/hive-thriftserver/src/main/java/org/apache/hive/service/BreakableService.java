/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service.STATE;

/**
 * This is a service that can be configured to break on any of the lifecycle
 * events, so test the failure handling of other parts of the service
 * infrastructure.
 *
 * It retains a counter to the number of times each entry point is called -
 * these counters are incremented before the exceptions are raised and
 * before the superclass state methods are invoked.
 *
 */
public class BreakableService extends AbstractService {
  private boolean failOnInit;
  private boolean failOnStart;
  private boolean failOnStop;
  private final int[] counts = new int[4];

  public BreakableService() {
    this(false, false, false);
  }

  public BreakableService(boolean failOnInit,
                          boolean failOnStart,
                          boolean failOnStop) {
    super("BreakableService");
    this.failOnInit = failOnInit;
    this.failOnStart = failOnStart;
    this.failOnStop = failOnStop;
    inc(STATE.NOTINITED);
  }

  private int convert(STATE state) {
    switch (state) {
      case NOTINITED: return 0;
      case INITED:    return 1;
      case STARTED:   return 2;
      case STOPPED:   return 3;
      default:        return 0;
    }
  }

  private void inc(STATE state) {
    int index = convert(state);
    counts[index] ++;
  }

  public int getCount(STATE state) {
    return counts[convert(state)];
  }

  private void maybeFail(boolean fail, String action) {
    if (fail) {
      throw new BrokenLifecycleEvent(action);
    }
  }

  @Override
  public void init(HiveConf conf) {
    inc(STATE.INITED);
    maybeFail(failOnInit, "init");
    super.init(conf);
  }

  @Override
  public void start() {
    inc(STATE.STARTED);
    maybeFail(failOnStart, "start");
    super.start();
  }

  @Override
  public void stop() {
    inc(STATE.STOPPED);
    maybeFail(failOnStop, "stop");
    super.stop();
  }

  public void setFailOnInit(boolean failOnInit) {
    this.failOnInit = failOnInit;
  }

  public void setFailOnStart(boolean failOnStart) {
    this.failOnStart = failOnStart;
  }

  public void setFailOnStop(boolean failOnStop) {
    this.failOnStop = failOnStop;
  }

  /**
   * The exception explicitly raised on a failure
   */
  public static class BrokenLifecycleEvent extends RuntimeException {
    BrokenLifecycleEvent(String action) {
      super("Lifecycle Failure during " + action);
    }
  }

}
