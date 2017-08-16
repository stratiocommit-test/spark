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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * CompositeService.
 *
 */
public class CompositeService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(CompositeService.class);

  private final List<Service> serviceList = new ArrayList<Service>();

  public CompositeService(String name) {
    super(name);
  }

  public Collection<Service> getServices() {
    return Collections.unmodifiableList(serviceList);
  }

  protected synchronized void addService(Service service) {
    serviceList.add(service);
  }

  protected synchronized boolean removeService(Service service) {
    return serviceList.remove(service);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    for (Service service : serviceList) {
      service.init(hiveConf);
    }
    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    int i = 0;
    try {
      for (int n = serviceList.size(); i < n; i++) {
        Service service = serviceList.get(i);
        service.start();
      }
      super.start();
    } catch (Throwable e) {
      LOG.error("Error starting services " + getName(), e);
      // Note that the state of the failed service is still INITED and not
      // STARTED. Even though the last service is not started completely, still
      // call stop() on all services including failed service to make sure cleanup
      // happens.
      stop(i);
      throw new ServiceException("Failed to Start " + getName(), e);
    }

  }

  @Override
  public synchronized void stop() {
    if (this.getServiceState() == STATE.STOPPED) {
      // The base composite-service is already stopped, don't do anything again.
      return;
    }
    if (serviceList.size() > 0) {
      stop(serviceList.size() - 1);
    }
    super.stop();
  }

  private synchronized void stop(int numOfServicesStarted) {
    // stop in reserve order of start
    for (int i = numOfServicesStarted; i >= 0; i--) {
      Service service = serviceList.get(i);
      try {
        service.stop();
      } catch (Throwable t) {
        LOG.info("Error stopping " + service.getName(), t);
      }
    }
  }

  /**
   * JVM Shutdown hook for CompositeService which will stop the given
   * CompositeService gracefully in case of JVM shutdown.
   */
  public static class CompositeServiceShutdownHook implements Runnable {

    private final CompositeService compositeService;

    public CompositeServiceShutdownHook(CompositeService compositeService) {
      this.compositeService = compositeService;
    }

    @Override
    public void run() {
      try {
        // Stop the Composite Service
        compositeService.stop();
      } catch (Throwable t) {
        LOG.info("Error stopping " + compositeService.getName(), t);
      }
    }
  }


}
