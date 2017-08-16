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
 * HiveSessionHookContext.
 * Interface passed to the HiveServer2 session hook execution. This enables
 * the hook implementation to access session config, user and session handle
 */
public interface HiveSessionHookContext {

  /**
   * Retrieve session conf
   * @return
   */
  HiveConf getSessionConf();

  /**
   * The get the username starting the session
   * @return
   */
  String getSessionUser();

  /**
   * Retrieve handle for the session
   * @return
   */
  String getSessionHandle();
}
