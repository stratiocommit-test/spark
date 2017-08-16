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

import scala.reflect.ClassTag

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

private[kinesis] class KinesisInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel,
    messageHandler: Record => T,
    awsCredentialsOption: Option[SerializableAWSCredentials]
  ) extends ReceiverInputDStream[T](_ssc) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges = blockInfos.map {
        _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
      logDebug(s"Creating KinesisBackedBlockRDD for $time with ${seqNumRanges.length} " +
          s"seq number ranges: ${seqNumRanges.mkString(", ")} ")
      new KinesisBackedBlockRDD(
        context.sc, regionName, endpointUrl, blockIds, seqNumRanges,
        isBlockIdValid = isBlockIdValid,
        retryTimeoutMs = ssc.graph.batchDuration.milliseconds.toInt,
        messageHandler = messageHandler,
        awsCredentialsOption = awsCredentialsOption)
    } else {
      logWarning("Kinesis sequence number information was not present with some block metadata," +
        " it may not be possible to recover from failures")
      super.createBlockRDD(time, blockInfos)
    }
  }

  override def getReceiver(): Receiver[T] = {
    new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream,
      checkpointAppName, checkpointInterval, storageLevel, messageHandler, awsCredentialsOption)
  }
}
