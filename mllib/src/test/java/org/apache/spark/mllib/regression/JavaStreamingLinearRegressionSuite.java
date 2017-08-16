/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.regression;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import static org.apache.spark.streaming.JavaTestUtils.*;

public class JavaStreamingLinearRegressionSuite {

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

  @Test
  @SuppressWarnings("unchecked")
  public void javaAPI() {
    List<LabeledPoint> trainingBatch = Arrays.asList(
      new LabeledPoint(1.0, Vectors.dense(1.0)),
      new LabeledPoint(0.0, Vectors.dense(0.0)));
    JavaDStream<LabeledPoint> training =
      attachTestInputStream(ssc, Arrays.asList(trainingBatch, trainingBatch), 2);
    List<Tuple2<Integer, Vector>> testBatch = Arrays.asList(
      new Tuple2<>(10, Vectors.dense(1.0)),
      new Tuple2<>(11, Vectors.dense(0.0)));
    JavaPairDStream<Integer, Vector> test = JavaPairDStream.fromJavaDStream(
      attachTestInputStream(ssc, Arrays.asList(testBatch, testBatch), 2));
    StreamingLinearRegressionWithSGD slr = new StreamingLinearRegressionWithSGD()
      .setNumIterations(2)
      .setInitialWeights(Vectors.dense(0.0));
    slr.trainOn(training);
    JavaPairDStream<Integer, Double> prediction = slr.predictOnValues(test);
    attachTestOutputStream(prediction.count());
    runStreams(ssc, 2, 2);
  }
}
