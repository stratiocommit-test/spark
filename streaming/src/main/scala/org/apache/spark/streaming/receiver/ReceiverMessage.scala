/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.receiver

import org.apache.spark.streaming.Time

/** Messages sent to the Receiver. */
private[streaming] sealed trait ReceiverMessage extends Serializable
private[streaming] object StopReceiver extends ReceiverMessage
private[streaming] case class CleanupOldBlocks(threshTime: Time) extends ReceiverMessage
private[streaming] case class UpdateRateLimit(elementsPerSecond: Long)
                   extends ReceiverMessage
