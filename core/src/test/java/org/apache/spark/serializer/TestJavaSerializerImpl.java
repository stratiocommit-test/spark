/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.serializer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import scala.reflect.ClassTag;


/**
 * A simple Serializer implementation to make sure the API is Java-friendly.
 */
class TestJavaSerializerImpl extends Serializer {

  @Override
  public SerializerInstance newInstance() {
    return null;
  }

  static class SerializerInstanceImpl extends SerializerInstance {
      @Override
      public <T> ByteBuffer serialize(T t, ClassTag<T> evidence$1) {
        return null;
      }

      @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public SerializationStream serializeStream(OutputStream s) {
      return null;
    }

    @Override
    public DeserializationStream deserializeStream(InputStream s) {
      return null;
    }
  }

  static class SerializationStreamImpl extends SerializationStream {

    @Override
    public <T> SerializationStream writeObject(T t, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }
  }

  static class DeserializationStreamImpl extends DeserializationStream {

    @Override
    public <T> T readObject(ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public void close() {

    }
  }
}
