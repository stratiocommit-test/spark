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
 * FilterService.
 *
 */
public class FilterService implements Service {


  private final Service service;
  private final long startTime = System.currentTimeMillis();

  public FilterService(Service service) {
    this.service = service;
  }

  @Override
  public void init(HiveConf config) {
    service.init(config);
  }

  @Override
  public void start() {
    service.start();
  }

  @Override
  public void stop() {
    service.stop();
  }


  @Override
  public void register(ServiceStateChangeListener listener) {
    service.register(listener);
  }

  @Override
  public void unregister(ServiceStateChangeListener listener) {
    service.unregister(listener);
  }

  @Override
  public String getName() {
    return service.getName();
  }

  @Override
  public HiveConf getHiveConf() {
    return service.getHiveConf();
  }

  @Override
  public STATE getServiceState() {
    return service.getServiceState();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

}
