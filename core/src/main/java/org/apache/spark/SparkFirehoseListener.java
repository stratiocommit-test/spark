/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark;

import org.apache.spark.scheduler.*;

/**
 * Class that allows users to receive all SparkListener events.
 * Users should override the onEvent method.
 *
 * This is a concrete Java class in order to ensure that we don't forget to update it when adding
 * new methods to SparkListener: forgetting to add a method will result in a compilation error (if
 * this was a concrete Scala class, default implementations of new event handlers would be inherited
 * from the SparkListener trait).
 */
public class SparkFirehoseListener implements SparkListenerInterface {

    public void onEvent(SparkListenerEvent event) { }

    @Override
    public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        onEvent(stageCompleted);
    }

    @Override
    public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        onEvent(stageSubmitted);
    }

    @Override
    public final void onTaskStart(SparkListenerTaskStart taskStart) {
        onEvent(taskStart);
    }

    @Override
    public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        onEvent(taskGettingResult);
    }

    @Override
    public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        onEvent(taskEnd);
    }

    @Override
    public final void onJobStart(SparkListenerJobStart jobStart) {
        onEvent(jobStart);
    }

    @Override
    public final void onJobEnd(SparkListenerJobEnd jobEnd) {
        onEvent(jobEnd);
    }

    @Override
    public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        onEvent(environmentUpdate);
    }

    @Override
    public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        onEvent(blockManagerAdded);
    }

    @Override
    public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        onEvent(blockManagerRemoved);
    }

    @Override
    public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        onEvent(unpersistRDD);
    }

    @Override
    public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        onEvent(applicationStart);
    }

    @Override
    public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        onEvent(applicationEnd);
    }

    @Override
    public final void onExecutorMetricsUpdate(
            SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        onEvent(executorMetricsUpdate);
    }

    @Override
    public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        onEvent(executorAdded);
    }

    @Override
    public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        onEvent(executorRemoved);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        onEvent(blockUpdated);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        onEvent(event);
    }
}
