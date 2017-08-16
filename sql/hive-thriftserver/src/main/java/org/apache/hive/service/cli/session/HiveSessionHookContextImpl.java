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

/**
 *
 * HiveSessionHookContextImpl.
 * Session hook context implementation which is created by session  manager
 * and passed to hook invocation.
 */
public class HiveSessionHookContextImpl implements HiveSessionHookContext {

  private final HiveSession hiveSession;

  HiveSessionHookContextImpl(HiveSession hiveSession) {
    this.hiveSession = hiveSession;
  }

  @Override
  public HiveConf getSessionConf() {
    return hiveSession.getHiveConf();
  }


  @Override
  public String getSessionUser() {
    return hiveSession.getUserName();
  }

  @Override
  public String getSessionHandle() {
    return hiveSession.getSessionHandle().toString();
  }
}
