/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

class NamedThreadFactory implements ThreadFactory {

  private final String nameFormat;
  private final AtomicLong threadIds;

  NamedThreadFactory(String nameFormat) {
    this.nameFormat = nameFormat;
    this.threadIds = new AtomicLong();
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r, String.format(nameFormat, threadIds.incrementAndGet()));
    t.setDaemon(true);
    return t;
  }

}
