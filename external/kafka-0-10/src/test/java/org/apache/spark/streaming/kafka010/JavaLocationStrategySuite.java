/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kafka010;

import java.io.Serializable;
import java.util.*;

import scala.collection.JavaConverters;

import org.apache.kafka.common.TopicPartition;

import org.junit.Assert;
import org.junit.Test;

public class JavaLocationStrategySuite implements Serializable {

  @Test
  public void testLocationStrategyConstructors() {
    final String topic1 = "topic1";
    final TopicPartition tp1 = new TopicPartition(topic1, 0);
    final TopicPartition tp2 = new TopicPartition(topic1, 1);
    final Map<TopicPartition, String> hosts = new HashMap<>();
    hosts.put(tp1, "node1");
    hosts.put(tp2, "node2");
    final scala.collection.Map<TopicPartition, String> sHosts =
      JavaConverters.mapAsScalaMapConverter(hosts).asScala();

    // make sure constructors can be called from java
    final LocationStrategy c1 = LocationStrategies.PreferConsistent();
    final LocationStrategy c2 = LocationStrategies.PreferConsistent();
    Assert.assertSame(c1, c2);

    final LocationStrategy c3 = LocationStrategies.PreferBrokers();
    final LocationStrategy c4 = LocationStrategies.PreferBrokers();
    Assert.assertSame(c3, c4);

    Assert.assertNotSame(c1, c3);

    final LocationStrategy c5 = LocationStrategies.PreferFixed(hosts);
    final LocationStrategy c6 = LocationStrategies.PreferFixed(sHosts);
    Assert.assertEquals(c5, c6);
  }

}
