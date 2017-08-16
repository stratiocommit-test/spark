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

/**
 * ServiceStateChangeListener.
 *
 */
public interface ServiceStateChangeListener {

  /**
   * Callback to notify of a state change. The service will already
   * have changed state before this callback is invoked.
   *
   * This operation is invoked on the thread that initiated the state change,
   * while the service itself in in a synchronized section.
   * <ol>
   *   <li>Any long-lived operation here will prevent the service state
   *   change from completing in a timely manner.</li>
   *   <li>If another thread is somehow invoked from the listener, and
   *   that thread invokes the methods of the service (including
   *   subclass-specific methods), there is a risk of a deadlock.</li>
   * </ol>
   *
   *
   * @param service the service that has changed.
   */
  void stateChanged(Service service);

}
