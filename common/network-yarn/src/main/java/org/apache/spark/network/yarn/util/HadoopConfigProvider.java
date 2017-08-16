/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.yarn.util;

import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;

import org.apache.spark.network.util.ConfigProvider;

/** Use the Hadoop configuration to obtain config values. */
public class HadoopConfigProvider extends ConfigProvider {
  private final Configuration conf;

  public HadoopConfigProvider(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String get(String name) {
    String value = conf.get(name);
    if (value == null) {
      throw new NoSuchElementException(name);
    }
    return value;
  }
}
