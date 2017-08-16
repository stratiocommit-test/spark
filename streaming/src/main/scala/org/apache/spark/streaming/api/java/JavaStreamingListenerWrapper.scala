/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.api.java

import scala.collection.JavaConverters._

import org.apache.spark.streaming.scheduler._

/**
 * A wrapper to convert a [[JavaStreamingListener]] to a [[StreamingListener]].
 */
private[streaming] class JavaStreamingListenerWrapper(javaStreamingListener: JavaStreamingListener)
  extends StreamingListener {

  private def toJavaReceiverInfo(receiverInfo: ReceiverInfo): JavaReceiverInfo = {
    JavaReceiverInfo(
      receiverInfo.streamId,
      receiverInfo.name,
      receiverInfo.active,
      receiverInfo.location,
      receiverInfo.executorId,
      receiverInfo.lastErrorMessage,
      receiverInfo.lastError,
      receiverInfo.lastErrorTime
    )
  }

  private def toJavaStreamInputInfo(streamInputInfo: StreamInputInfo): JavaStreamInputInfo = {
    JavaStreamInputInfo(
      streamInputInfo.inputStreamId,
      streamInputInfo.numRecords: Long,
      streamInputInfo.metadata.asJava,
      streamInputInfo.metadataDescription.orNull
    )
  }

  private def toJavaOutputOperationInfo(
      outputOperationInfo: OutputOperationInfo): JavaOutputOperationInfo = {
    JavaOutputOperationInfo(
      outputOperationInfo.batchTime,
      outputOperationInfo.id,
      outputOperationInfo.name,
      outputOperationInfo.description: String,
      outputOperationInfo.startTime.getOrElse(-1),
      outputOperationInfo.endTime.getOrElse(-1),
      outputOperationInfo.failureReason.orNull
    )
  }

  private def toJavaBatchInfo(batchInfo: BatchInfo): JavaBatchInfo = {
    JavaBatchInfo(
      batchInfo.batchTime,
      batchInfo.streamIdToInputInfo.mapValues(toJavaStreamInputInfo(_)).asJava,
      batchInfo.submissionTime,
      batchInfo.processingStartTime.getOrElse(-1),
      batchInfo.processingEndTime.getOrElse(-1),
      batchInfo.schedulingDelay.getOrElse(-1),
      batchInfo.processingDelay.getOrElse(-1),
      batchInfo.totalDelay.getOrElse(-1),
      batchInfo.numRecords,
      batchInfo.outputOperationInfos.mapValues(toJavaOutputOperationInfo(_)).asJava
    )
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    javaStreamingListener.onReceiverStarted(
      new JavaStreamingListenerReceiverStarted(toJavaReceiverInfo(receiverStarted.receiverInfo)))
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    javaStreamingListener.onReceiverError(
      new JavaStreamingListenerReceiverError(toJavaReceiverInfo(receiverError.receiverInfo)))
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    javaStreamingListener.onReceiverStopped(
      new JavaStreamingListenerReceiverStopped(toJavaReceiverInfo(receiverStopped.receiverInfo)))
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    javaStreamingListener.onBatchSubmitted(
      new JavaStreamingListenerBatchSubmitted(toJavaBatchInfo(batchSubmitted.batchInfo)))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    javaStreamingListener.onBatchStarted(
      new JavaStreamingListenerBatchStarted(toJavaBatchInfo(batchStarted.batchInfo)))
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    javaStreamingListener.onBatchCompleted(
      new JavaStreamingListenerBatchCompleted(toJavaBatchInfo(batchCompleted.batchInfo)))
  }

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    javaStreamingListener.onOutputOperationStarted(new JavaStreamingListenerOutputOperationStarted(
      toJavaOutputOperationInfo(outputOperationStarted.outputOperationInfo)))
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    javaStreamingListener.onOutputOperationCompleted(
      new JavaStreamingListenerOutputOperationCompleted(
        toJavaOutputOperationInfo(outputOperationCompleted.outputOperationInfo)))
  }

}
