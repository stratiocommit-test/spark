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

/**
 * Service.
 *
 */
public interface Service {

  /**
   * Service states
   */
  enum STATE {
    /** Constructed but not initialized */
    NOTINITED,

    /** Initialized but not started or stopped */
    INITED,

    /** started and not stopped */
    STARTED,

    /** stopped. No further state transitions are permitted */
    STOPPED
  }

  /**
   * Initialize the service.
   *
   * The transition must be from {@link STATE#NOTINITED} to {@link STATE#INITED} unless the
   * operation failed and an exception was raised.
   *
   * @param config
   *          the configuration of the service
   */
  void init(HiveConf conf);


  /**
   * Start the service.
   *
   * The transition should be from {@link STATE#INITED} to {@link STATE#STARTED} unless the
   * operation failed and an exception was raised.
   */
  void start();

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  void stop();

  /**
   * Register an instance of the service state change events.
   *
   * @param listener
   *          a new listener
   */
  void register(ServiceStateChangeListener listener);

  /**
   * Unregister a previously instance of the service state change events.
   *
   * @param listener
   *          the listener to unregister.
   */
  void unregister(ServiceStateChangeListener listener);

  /**
   * Get the name of this service.
   *
   * @return the service name
   */
  String getName();

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   *
   * @return the current configuration, unless a specific implementation chooses
   *         otherwise.
   */
  HiveConf getHiveConf();

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  STATE getServiceState();

  /**
   * Get the service start time
   *
   * @return the start time of the service. This will be zero if the service
   *         has not yet been started.
   */
  long getStartTime();

}
