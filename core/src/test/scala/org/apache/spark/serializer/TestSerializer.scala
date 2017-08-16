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

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * A serializer implementation that always returns two elements in a deserialization stream.
 */
class TestSerializer extends Serializer {
  override def newInstance(): TestSerializerInstance = new TestSerializerInstance
}


class TestSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  override def deserializeStream(s: InputStream): TestDeserializationStream =
    new TestDeserializationStream

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}


class TestDeserializationStream extends DeserializationStream {

  private var count = 0

  override def readObject[T: ClassTag](): T = {
    count += 1
    if (count == 3) {
      throw new EOFException
    }
    new Object().asInstanceOf[T]
  }

  override def close(): Unit = {}
}
