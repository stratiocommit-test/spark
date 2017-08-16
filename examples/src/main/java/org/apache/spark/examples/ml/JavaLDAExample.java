/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.ml;
// $example on$
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

/**
 * An example demonstrating LDA.
 * Run with
 * <pre>
 * bin/run-example ml.JavaLDAExample
 * </pre>
 */
public class JavaLDAExample {

  public static void main(String[] args) {
    // Creates a SparkSession
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLDAExample")
      .getOrCreate();

    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm")
      .load("data/mllib/sample_lda_libsvm_data.txt");

    // Trains a LDA model.
    LDA lda = new LDA().setK(10).setMaxIter(10);
    LDAModel model = lda.fit(dataset);

    double ll = model.logLikelihood(dataset);
    double lp = model.logPerplexity(dataset);
    System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
    System.out.println("The upper bound bound on perplexity: " + lp);

    // Describe topics.
    Dataset<Row> topics = model.describeTopics(3);
    System.out.println("The topics described by their top-weighted terms:");
    topics.show(false);

    // Shows the result.
    Dataset<Row> transformed = model.transform(dataset);
    transformed.show(false);
    // $example off$

    spark.stop();
  }
}
