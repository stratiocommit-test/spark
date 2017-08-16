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
import java.util.Arrays;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$
import org.apache.spark.api.java.function.VoidFunction;

public class JavaElementwiseProductExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaElementwiseProductExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Create some vector data; also works for sparse vectors
    JavaRDD<Vector> data = jsc.parallelize(Arrays.asList(
      Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)));
    Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);
    final ElementwiseProduct transformer = new ElementwiseProduct(transformingVector);

    // Batch transform and per-row transform give the same results:
    JavaRDD<Vector> transformedData = transformer.transform(data);
    JavaRDD<Vector> transformedData2 = data.map(
      new Function<Vector, Vector>() {
        @Override
        public Vector call(Vector v) {
          return transformer.transform(v);
        }
      }
    );
    // $example off$

    System.out.println("transformedData: ");
    transformedData.foreach(new VoidFunction<Vector>() {
      @Override
      public void call(Vector vector) throws Exception {
        System.out.println(vector.toString());
      }
    });

    System.out.println("transformedData2: ");
    transformedData2.foreach(new VoidFunction<Vector>() {
      @Override
      public void call(Vector vector) throws Exception {
        System.out.println(vector.toString());
      }
    });

    jsc.stop();
  }
}
