/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.mllib;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class JavaBinaryClassificationMetricsExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Java Binary Classification Metrics Example");
    SparkContext sc = new SparkContext(conf);
    // $example on$
    String path = "data/mllib/sample_binary_classification_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits =
      data.randomSplit(new double[]{0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training.rdd());

    // Clear the prediction threshold so the model will return probabilities
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        @Override
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = model.predict(p.features());
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics =
      new BinaryClassificationMetrics(predictionAndLabels.rdd());

    // Precision by threshold
    JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
    System.out.println("Precision by threshold: " + precision.collect());

    // Recall by threshold
    JavaRDD<Tuple2<Object, Object>> recall = metrics.recallByThreshold().toJavaRDD();
    System.out.println("Recall by threshold: " + recall.collect());

    // F Score by threshold
    JavaRDD<Tuple2<Object, Object>> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
    System.out.println("F1 Score by threshold: " + f1Score.collect());

    JavaRDD<Tuple2<Object, Object>> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
    System.out.println("F2 Score by threshold: " + f2Score.collect());

    // Precision-recall curve
    JavaRDD<Tuple2<Object, Object>> prc = metrics.pr().toJavaRDD();
    System.out.println("Precision-recall curve: " + prc.collect());

    // Thresholds
    JavaRDD<Double> thresholds = precision.map(
      new Function<Tuple2<Object, Object>, Double>() {
        @Override
        public Double call(Tuple2<Object, Object> t) {
          return new Double(t._1().toString());
        }
      }
    );

    // ROC Curve
    JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
    System.out.println("ROC curve: " + roc.collect());

    // AUPRC
    System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

    // AUROC
    System.out.println("Area under ROC = " + metrics.areaUnderROC());

    // Save and load model
    model.save(sc, "target/tmp/LogisticRegressionModel");
    LogisticRegressionModel.load(sc, "target/tmp/LogisticRegressionModel");
    // $example off$
  }
}
