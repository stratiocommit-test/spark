/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming;

import org.apache.spark.streaming.api.java.*;

public class JavaStreamingListenerAPISuite extends JavaStreamingListener {

  @Override
  public void onReceiverStarted(JavaStreamingListenerReceiverStarted receiverStarted) {
    JavaReceiverInfo receiverInfo = receiverStarted.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onReceiverError(JavaStreamingListenerReceiverError receiverError) {
    JavaReceiverInfo receiverInfo = receiverError.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onReceiverStopped(JavaStreamingListenerReceiverStopped receiverStopped) {
    JavaReceiverInfo receiverInfo = receiverStopped.receiverInfo();
    receiverInfo.streamId();
    receiverInfo.name();
    receiverInfo.active();
    receiverInfo.location();
    receiverInfo.executorId();
    receiverInfo.lastErrorMessage();
    receiverInfo.lastError();
    receiverInfo.lastErrorTime();
  }

  @Override
  public void onBatchSubmitted(JavaStreamingListenerBatchSubmitted batchSubmitted) {
    super.onBatchSubmitted(batchSubmitted);
  }

  @Override
  public void onBatchStarted(JavaStreamingListenerBatchStarted batchStarted) {
    super.onBatchStarted(batchStarted);
  }

  @Override
  public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
    super.onBatchCompleted(batchCompleted);
  }

  @Override
  public void onOutputOperationStarted(
      JavaStreamingListenerOutputOperationStarted outputOperationStarted) {
    super.onOutputOperationStarted(outputOperationStarted);
  }

  @Override
  public void onOutputOperationCompleted(
      JavaStreamingListenerOutputOperationCompleted outputOperationCompleted) {
    super.onOutputOperationCompleted(outputOperationCompleted);
  }
}
