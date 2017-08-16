/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui.jobs

/**
 * Names of the CSS classes corresponding to each type of task detail. Used to allow users
 * to optionally show/hide columns.
 *
 * If new optional metrics are added here, they should also be added to the end of webui.css
 * to have the style set to "display: none;" by default.
 */
private[spark] object TaskDetailsClassNames {
  val SCHEDULER_DELAY = "scheduler_delay"
  val TASK_DESERIALIZATION_TIME = "deserialization_time"
  val SHUFFLE_READ_BLOCKED_TIME = "fetch_wait_time"
  val SHUFFLE_READ_REMOTE_SIZE = "shuffle_read_remote"
  val RESULT_SERIALIZATION_TIME = "serialization_time"
  val GETTING_RESULT_TIME = "getting_result_time"
  val PEAK_EXECUTION_MEMORY = "peak_execution_memory"
}
