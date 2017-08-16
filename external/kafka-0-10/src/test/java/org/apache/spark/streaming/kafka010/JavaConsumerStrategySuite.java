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
import java.util.regex.Pattern;

import scala.collection.JavaConverters;

import org.apache.kafka.common.TopicPartition;

import org.junit.Assert;
import org.junit.Test;

public class JavaConsumerStrategySuite implements Serializable {

  @Test
  public void testConsumerStrategyConstructors() {
    final String topic1 = "topic1";
    final Pattern pat = Pattern.compile("top.*");
    final Collection<String> topics = Arrays.asList(topic1);
    final scala.collection.Iterable<String> sTopics =
      JavaConverters.collectionAsScalaIterableConverter(topics).asScala();
    final TopicPartition tp1 = new TopicPartition(topic1, 0);
    final TopicPartition tp2 = new TopicPartition(topic1, 1);
    final Collection<TopicPartition> parts = Arrays.asList(tp1, tp2);
    final scala.collection.Iterable<TopicPartition> sParts =
      JavaConverters.collectionAsScalaIterableConverter(parts).asScala();
    final Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put("bootstrap.servers", "not used");
    final scala.collection.Map<String, Object> sKafkaParams =
      JavaConverters.mapAsScalaMapConverter(kafkaParams).asScala();
    final Map<TopicPartition, Long> offsets = new HashMap<>();
    offsets.put(tp1, 23L);
    final scala.collection.Map<TopicPartition, Object> sOffsets =
      JavaConverters.mapAsScalaMapConverter(offsets).asScala().mapValues(
        new scala.runtime.AbstractFunction1<Long, Object>() {
          @Override
          public Object apply(Long x) {
            return (Object) x;
          }
        }
      );

    final ConsumerStrategy<String, String> sub1 =
      ConsumerStrategies.<String, String>Subscribe(sTopics, sKafkaParams, sOffsets);
    final ConsumerStrategy<String, String> sub2 =
      ConsumerStrategies.<String, String>Subscribe(sTopics, sKafkaParams);
    final ConsumerStrategy<String, String> sub3 =
      ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, offsets);
    final ConsumerStrategy<String, String> sub4 =
      ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams);

    Assert.assertEquals(
      sub1.executorKafkaParams().get("bootstrap.servers"),
      sub3.executorKafkaParams().get("bootstrap.servers"));

    final ConsumerStrategy<String, String> psub1 =
      ConsumerStrategies.<String, String>SubscribePattern(pat, sKafkaParams, sOffsets);
    final ConsumerStrategy<String, String> psub2 =
      ConsumerStrategies.<String, String>SubscribePattern(pat, sKafkaParams);
    final ConsumerStrategy<String, String> psub3 =
      ConsumerStrategies.<String, String>SubscribePattern(pat, kafkaParams, offsets);
    final ConsumerStrategy<String, String> psub4 =
      ConsumerStrategies.<String, String>SubscribePattern(pat, kafkaParams);

    Assert.assertEquals(
      psub1.executorKafkaParams().get("bootstrap.servers"),
      psub3.executorKafkaParams().get("bootstrap.servers"));

    final ConsumerStrategy<String, String> asn1 =
      ConsumerStrategies.<String, String>Assign(sParts, sKafkaParams, sOffsets);
    final ConsumerStrategy<String, String> asn2 =
      ConsumerStrategies.<String, String>Assign(sParts, sKafkaParams);
    final ConsumerStrategy<String, String> asn3 =
      ConsumerStrategies.<String, String>Assign(parts, kafkaParams, offsets);
    final ConsumerStrategy<String, String> asn4 =
      ConsumerStrategies.<String, String>Assign(parts, kafkaParams);

    Assert.assertEquals(
      asn1.executorKafkaParams().get("bootstrap.servers"),
      asn3.executorKafkaParams().get("bootstrap.servers"));
  }

}
