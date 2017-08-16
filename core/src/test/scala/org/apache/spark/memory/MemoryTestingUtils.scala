/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.memory

import java.util.Properties

import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}

/**
 * Helper methods for mocking out memory-management-related classes in tests.
 */
object MemoryTestingUtils {
  def fakeTaskContext(env: SparkEnv): TaskContext = {
    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0)
    new TaskContextImpl(
      stageId = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = env.metricsSystem)
  }
}
