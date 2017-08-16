/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kinesis

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.services.kinesis.producer.{KinesisProducer => KPLProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}

private[kinesis] class KPLBasedKinesisTestUtils extends KinesisTestUtils {
  override protected def getProducer(aggregate: Boolean): KinesisDataGenerator = {
    if (!aggregate) {
      new SimpleDataGenerator(kinesisClient)
    } else {
      new KPLDataGenerator(regionName)
    }
  }
}

/** A wrapper for the KinesisProducer provided in the KPL. */
private[kinesis] class KPLDataGenerator(regionName: String) extends KinesisDataGenerator {

  private lazy val producer: KPLProducer = {
    val conf = new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(1000)
      .setMaxConnections(1)
      .setRegion(regionName)
      .setMetricsLevel("none")

    new KPLProducer(conf)
  }

  override def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()
    data.foreach { num =>
      val str = num.toString
      val data = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
      val future = producer.addUserRecord(streamName, str, data)
      val kinesisCallBack = new FutureCallback[UserRecordResult]() {
        override def onFailure(t: Throwable): Unit = {} // do nothing

        override def onSuccess(result: UserRecordResult): Unit = {
          val shardId = result.getShardId
          val seqNumber = result.getSequenceNumber()
          val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
            new ArrayBuffer[(Int, String)]())
          sentSeqNumbers += ((num, seqNumber))
        }
      }
      Futures.addCallback(future, kinesisCallBack)
    }
    producer.flushSync()
    shardIdToSeqNumbers.toMap
  }
}
