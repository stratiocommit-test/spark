/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.serializer

import com.esotericsoftware.kryo.Kryo

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.serializer.KryoDistributedTest._
import org.apache.spark.util.Utils

class KryoSerializerDistributedSuite extends SparkFunSuite with LocalSparkContext {

  test("kryo objects are serialised consistently in different processes") {
    val conf = new SparkConf(false)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[AppJarRegistrator].getName)
      .set(config.MAX_TASK_FAILURES, 1)
      .set(config.BLACKLIST_ENABLED, false)

    val jar = TestUtils.createJarWithClasses(List(AppJarRegistrator.customClassName))
    conf.setJars(List(jar.getPath))

    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    val original = Thread.currentThread.getContextClassLoader
    val loader = new java.net.URLClassLoader(Array(jar), Utils.getContextOrSparkClassLoader)
    SparkEnv.get.serializer.setDefaultClassLoader(loader)

    val cachedRDD = sc.parallelize((0 until 10).map((_, new MyCustomClass)), 3).cache()

    // Randomly mix the keys so that the join below will require a shuffle with each partition
    // sending data to multiple other partitions.
    val shuffledRDD = cachedRDD.map { case (i, o) => (i * i * i - 10 * i * i, o)}

    // Join the two RDDs, and force evaluation
    assert(shuffledRDD.join(cachedRDD).collect().size == 1)
  }
}

object KryoDistributedTest {
  class MyCustomClass

  class AppJarRegistrator extends KryoRegistrator {
    override def registerClasses(k: Kryo) {
      val classLoader = Thread.currentThread.getContextClassLoader
      // scalastyle:off classforname
      k.register(Class.forName(AppJarRegistrator.customClassName, true, classLoader))
      // scalastyle:on classforname
    }
  }

  object AppJarRegistrator {
    val customClassName = "KryoSerializerDistributedSuiteCustomClass"
  }
}
