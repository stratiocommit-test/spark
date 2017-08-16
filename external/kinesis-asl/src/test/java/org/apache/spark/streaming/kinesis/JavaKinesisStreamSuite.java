/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kinesis;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.model.Record;
import org.junit.Test;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * Demonstrate the use of the KinesisUtils Java API
 */
public class JavaKinesisStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testKinesisStream() {
    String dummyEndpointUrl = KinesisTestUtils.defaultEndpointUrl();
    String dummyRegionName = RegionUtils.getRegionByEndpoint(dummyEndpointUrl).getName();

    // Tests the API, does not actually test data receiving
    JavaDStream<byte[]> kinesisStream = KinesisUtils.createStream(ssc, "myAppName", "mySparkStream",
        dummyEndpointUrl, dummyRegionName, InitialPositionInStream.LATEST, new Duration(2000),
        StorageLevel.MEMORY_AND_DISK_2());
    ssc.stop();
  }


  private static Function<Record, String> handler = new Function<Record, String>() {
    @Override
    public String call(Record record) {
      return record.getPartitionKey() + "-" + record.getSequenceNumber();
    }
  };

  @Test
  public void testCustomHandler() {
    // Tests the API, does not actually test data receiving
    JavaDStream<String> kinesisStream = KinesisUtils.createStream(ssc, "testApp", "mySparkStream",
        "https://kinesis.us-west-2.amazonaws.com", "us-west-2", InitialPositionInStream.LATEST,
        new Duration(2000), StorageLevel.MEMORY_AND_DISK_2(), handler, String.class);

    ssc.stop();
  }
}
