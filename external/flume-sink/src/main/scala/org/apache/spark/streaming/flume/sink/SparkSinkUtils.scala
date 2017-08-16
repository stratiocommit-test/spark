/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.flume.sink

private[flume] object SparkSinkUtils {
  /**
   * This method determines if this batch represents an error or not.
   * @param batch - The batch to check
   * @return - true if the batch represents an error
   */
  def isErrorBatch(batch: EventBatch): Boolean = {
    !batch.getErrorMsg.toString.equals("") // If there is an error message, it is an error batch.
  }
}
