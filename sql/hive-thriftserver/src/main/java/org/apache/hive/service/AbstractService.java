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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * AbstractService.
 *
 */
public abstract class AbstractService implements Service {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private STATE state = STATE.NOTINITED;

  /**
   * Service name.
   */
  private final String name;
  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private HiveConf hiveConf;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final List<ServiceStateChangeListener> listeners =
      new ArrayList<ServiceStateChangeListener>();

  /**
   * Construct the service.
   *
   * @param name
   *          service name
   */
  public AbstractService(String name) {
    this.name = name;
  }

  @Override
  public synchronized STATE getServiceState() {
    return state;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    ensureCurrentState(STATE.NOTINITED);
    this.hiveConf = hiveConf;
    changeState(STATE.INITED);
    LOG.info("Service:" + getName() + " is inited.");
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void start() {
    startTime = System.currentTimeMillis();
    ensureCurrentState(STATE.INITED);
    changeState(STATE.STARTED);
    LOG.info("Service:" + getName() + " is started.");
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException
   *           if the current service state does not permit
   *           this action
   */
  @Override
  public synchronized void stop() {
    if (state == STATE.STOPPED ||
        state == STATE.INITED ||
        state == STATE.NOTINITED) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return;
    }
    ensureCurrentState(STATE.STARTED);
    changeState(STATE.STOPPED);
    LOG.info("Service:" + getName() + " is stopped.");
  }

  @Override
  public synchronized void register(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public synchronized void unregister(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized HiveConf getHiveConf() {
    return hiveConf;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Verify that that a service is in a given state.
   *
   * @param currentState
   *          the desired state
   * @throws IllegalStateException
   *           if the service state is different from
   *           the desired state
   */
  private void ensureCurrentState(STATE currentState) {
    ServiceOperations.ensureCurrentState(state, currentState);
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods,
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   *
   * @param newState
   *          new service state
   */
  private void changeState(STATE newState) {
    state = newState;
    // notify listeners
    for (ServiceStateChangeListener l : listeners) {
      l.stateChanged(this);
    }
  }

}
