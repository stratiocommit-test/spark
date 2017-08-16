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

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;

public abstract class LocalJavaStreamingContext {

    protected transient JavaStreamingContext ssc;

    @Before
    public void setUp() {
        SparkConf conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("test")
            .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        ssc = new JavaStreamingContext(conf, new Duration(1000));
        ssc.checkpoint("checkpoint");
    }

    @After
    public void tearDown() {
        ssc.stop();
        ssc = null;
    }
}
