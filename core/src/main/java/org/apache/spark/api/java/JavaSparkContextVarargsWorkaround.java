/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.java;

import java.util.ArrayList;
import java.util.List;

// See
// http://scala-programming-language.1934581.n4.nabble.com/Workaround-for-implementing-java-varargs-in-2-7-2-final-tp1944767p1944772.html
abstract class JavaSparkContextVarargsWorkaround {

  @SafeVarargs
  public final <T> JavaRDD<T> union(JavaRDD<T>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaRDD<T>> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  public JavaDoubleRDD union(JavaDoubleRDD... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaDoubleRDD> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  @SafeVarargs
  public final <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaPairRDD<K, V>> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  // These methods take separate "first" and "rest" elements to avoid having the same type erasure
  public abstract <T> JavaRDD<T> union(JavaRDD<T> first, List<JavaRDD<T>> rest);
  public abstract JavaDoubleRDD union(JavaDoubleRDD first, List<JavaDoubleRDD> rest);
  public abstract <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V> first, List<JavaPairRDD<K, V>>
    rest);
}
