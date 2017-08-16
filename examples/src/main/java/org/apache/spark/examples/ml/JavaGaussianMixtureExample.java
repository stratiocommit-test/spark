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
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;


/**
 * An example demonstrating Gaussian Mixture Model.
 * Run with
 * <pre>
 * bin/run-example ml.JavaGaussianMixtureExample
 * </pre>
 */
public class JavaGaussianMixtureExample {

  public static void main(String[] args) {

    // Creates a SparkSession
    SparkSession spark = SparkSession
            .builder()
            .appName("JavaGaussianMixtureExample")
            .getOrCreate();

    // $example on$
    // Loads data
    Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

    // Trains a GaussianMixture model
    GaussianMixture gmm = new GaussianMixture()
      .setK(2);
    GaussianMixtureModel model = gmm.fit(dataset);

    // Output the parameters of the mixture model
    for (int i = 0; i < model.getK(); i++) {
      System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
              i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
    }
    // $example off$

    spark.stop();
  }
}
