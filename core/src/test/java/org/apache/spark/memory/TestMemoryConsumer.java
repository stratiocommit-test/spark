/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.memory;

import java.io.IOException;

public class TestMemoryConsumer extends MemoryConsumer {
  public TestMemoryConsumer(TaskMemoryManager memoryManager, MemoryMode mode) {
    super(memoryManager, 1024L, mode);
  }
  public TestMemoryConsumer(TaskMemoryManager memoryManager) {
    this(memoryManager, MemoryMode.ON_HEAP);
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    long used = getUsed();
    free(used);
    return used;
  }

  void use(long size) {
    long got = taskMemoryManager.acquireExecutionMemory(size, this);
    used += got;
  }

  void free(long size) {
    used -= size;
    taskMemoryManager.releaseExecutionMemory(size, this);
  }
}


