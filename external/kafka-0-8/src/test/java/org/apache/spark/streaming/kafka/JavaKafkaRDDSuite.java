/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class JavaKafkaRDDSuite implements Serializable {
  private transient JavaSparkContext sc = null;
  private transient KafkaTestUtils kafkaTestUtils = null;

  @Before
  public void setUp() {
    kafkaTestUtils = new KafkaTestUtils();
    kafkaTestUtils.setup();
    SparkConf sparkConf = new SparkConf()
      .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
    sc = new JavaSparkContext(sparkConf);
  }

  @After
  public void tearDown() {
    if (sc != null) {
      sc.stop();
      sc = null;
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown();
      kafkaTestUtils = null;
    }
  }

  @Test
  public void testKafkaRDD() throws InterruptedException {
    String topic1 = "topic1";
    String topic2 = "topic2";

    createTopicAndSendData(topic1);
    createTopicAndSendData(topic2);

    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", kafkaTestUtils.brokerAddress());

    OffsetRange[] offsetRanges = {
      OffsetRange.create(topic1, 0, 0, 1),
      OffsetRange.create(topic2, 0, 0, 1)
    };

    Map<TopicAndPartition, Broker> emptyLeaders = new HashMap<>();
    Map<TopicAndPartition, Broker> leaders = new HashMap<>();
    String[] hostAndPort = kafkaTestUtils.brokerAddress().split(":");
    Broker broker = Broker.create(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    leaders.put(new TopicAndPartition(topic1, 0), broker);
    leaders.put(new TopicAndPartition(topic2, 0), broker);

    JavaRDD<String> rdd1 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        offsetRanges
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(Tuple2<String, String> kv) {
            return kv._2();
          }
        }
    );

    JavaRDD<String> rdd2 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        String.class,
        kafkaParams,
        offsetRanges,
        emptyLeaders,
        new Function<MessageAndMetadata<String, String>, String>() {
          @Override
          public String call(MessageAndMetadata<String, String> msgAndMd) {
            return msgAndMd.message();
          }
        }
    );

    JavaRDD<String> rdd3 = KafkaUtils.createRDD(
        sc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        String.class,
        kafkaParams,
        offsetRanges,
        leaders,
        new Function<MessageAndMetadata<String, String>, String>() {
          @Override
          public String call(MessageAndMetadata<String, String> msgAndMd) {
            return msgAndMd.message();
          }
        }
    );

    // just making sure the java user apis work; the scala tests handle logic corner cases
    long count1 = rdd1.count();
    long count2 = rdd2.count();
    long count3 = rdd3.count();
    Assert.assertTrue(count1 > 0);
    Assert.assertEquals(count1, count2);
    Assert.assertEquals(count1, count3);
  }

  private  String[] createTopicAndSendData(String topic) {
    String[] data = { topic + "-1", topic + "-2", topic + "-3"};
    kafkaTestUtils.createTopic(topic, 1);
    kafkaTestUtils.sendMessages(topic, data);
    return data;
  }
}
