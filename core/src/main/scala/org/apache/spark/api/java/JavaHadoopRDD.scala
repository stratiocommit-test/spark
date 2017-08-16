/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.java

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.mapred.InputSplit

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaSparkContext._
import org.apache.spark.api.java.function.{Function2 => JFunction2}
import org.apache.spark.rdd.HadoopRDD

@DeveloperApi
class JavaHadoopRDD[K, V](rdd: HadoopRDD[K, V])
    (implicit override val kClassTag: ClassTag[K], implicit override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd) {

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[R](
      f: JFunction2[InputSplit, java.util.Iterator[(K, V)], java.util.Iterator[R]],
      preservesPartitioning: Boolean = false): JavaRDD[R] = {
    new JavaRDD(rdd.mapPartitionsWithInputSplit((a, b) => f.call(a, b.asJava).asScala,
      preservesPartitioning)(fakeClassTag))(fakeClassTag)
  }
}
