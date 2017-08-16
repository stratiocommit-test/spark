/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.flume;

import java.net.InetSocketAddress;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Test;

public class JavaFlumePollingStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testFlumeStream() {
    // tests the API, does not actually test data receiving
    InetSocketAddress[] addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 12345)
    };
    JavaReceiverInputDStream<SparkFlumeEvent> test1 =
        FlumeUtils.createPollingStream(ssc, "localhost", 12345);
    JavaReceiverInputDStream<SparkFlumeEvent> test2 = FlumeUtils.createPollingStream(
        ssc, "localhost", 12345, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<SparkFlumeEvent> test3 = FlumeUtils.createPollingStream(
        ssc, addresses, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<SparkFlumeEvent> test4 = FlumeUtils.createPollingStream(
        ssc, addresses, StorageLevel.MEMORY_AND_DISK_SER_2(), 100, 5);
  }
}
