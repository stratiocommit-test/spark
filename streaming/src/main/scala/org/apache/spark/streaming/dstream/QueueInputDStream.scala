/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.dstream

import java.io.{NotSerializableException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.reflect.ClassTag

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{StreamingContext, Time}

private[streaming]
class QueueInputDStream[T: ClassTag](
    ssc: StreamingContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
  ) extends InputDStream[T](ssc) {

  override def start() { }

  override def stop() { }

  private def readObject(in: ObjectInputStream): Unit = {
    throw new NotSerializableException("queueStream doesn't support checkpointing. " +
      "Please don't use queueStream when checkpointing is enabled.")
  }

  private def writeObject(oos: ObjectOutputStream): Unit = {
    logWarning("queueStream doesn't support checkpointing")
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    queue.synchronized {
      if (oneAtATime && queue.nonEmpty) {
        buffer += queue.dequeue()
      } else {
        buffer ++= queue
        queue.clear()
      }
    }
    if (buffer.nonEmpty) {
      if (oneAtATime) {
        Some(buffer.head)
      } else {
        Some(new UnionRDD(context.sc, buffer.toSeq))
      }
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      Some(ssc.sparkContext.emptyRDD)
    }
  }

}
