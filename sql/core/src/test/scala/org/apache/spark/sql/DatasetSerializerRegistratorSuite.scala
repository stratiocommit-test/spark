/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.TestSparkSession

/**
 * Test suite to test Kryo custom registrators.
 */
class DatasetSerializerRegistratorSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /**
   * Initialize the [[TestSparkSession]] with a [[KryoRegistrator]].
   */
  protected override def beforeAll(): Unit = {
    sparkConf.set("spark.kryo.registrator", TestRegistrator().getClass.getCanonicalName)
    super.beforeAll()
  }

  test("Kryo registrator") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()
    assert(ds.collect().toSet == Set(KryoData(0), KryoData(0)))
  }

}

/** Used to test user provided registrator. */
class TestRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit =
    kryo.register(classOf[KryoData], new ZeroKryoDataSerializer())
}

object TestRegistrator {
  def apply(): TestRegistrator = new TestRegistrator()
}

/** A [[Serializer]] that takes a [[KryoData]] and serializes it as KryoData(0). */
class ZeroKryoDataSerializer extends Serializer[KryoData] {
  override def write(kryo: Kryo, output: Output, t: KryoData): Unit = {
    output.writeInt(0)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[KryoData]): KryoData = {
    KryoData(input.readInt())
  }
}
