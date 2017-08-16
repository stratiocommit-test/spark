/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.io

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.spark.SparkFunSuite


class ChunkedByteBufferOutputStreamSuite extends SparkFunSuite {

  test("empty output") {
    val o = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
    o.close()
    assert(o.toChunkedByteBuffer.size === 0)
  }

  test("write a single byte") {
    val o = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
    o.write(10)
    o.close()
    val chunkedByteBuffer = o.toChunkedByteBuffer
    assert(chunkedByteBuffer.getChunks().length === 1)
    assert(chunkedByteBuffer.getChunks().head.array().toSeq === Seq(10.toByte))
  }

  test("write a single near boundary") {
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(new Array[Byte](9))
    o.write(99)
    o.close()
    val chunkedByteBuffer = o.toChunkedByteBuffer
    assert(chunkedByteBuffer.getChunks().length === 1)
    assert(chunkedByteBuffer.getChunks().head.array()(9) === 99.toByte)
  }

  test("write a single at boundary") {
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(new Array[Byte](10))
    o.write(99)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 2)
    assert(arrays(1).length === 1)
    assert(arrays(1)(0) === 99.toByte)
  }

  test("single chunk output") {
    val ref = new Array[Byte](8)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 1)
    assert(arrays.head.length === ref.length)
    assert(arrays.head.toSeq === ref.toSeq)
  }

  test("single chunk output at boundary size") {
    val ref = new Array[Byte](10)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 1)
    assert(arrays.head.length === ref.length)
    assert(arrays.head.toSeq === ref.toSeq)
  }

  test("multiple chunk output") {
    val ref = new Array[Byte](26)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 3)
    assert(arrays(0).length === 10)
    assert(arrays(1).length === 10)
    assert(arrays(2).length === 6)

    assert(arrays(0).toSeq === ref.slice(0, 10))
    assert(arrays(1).toSeq === ref.slice(10, 20))
    assert(arrays(2).toSeq === ref.slice(20, 26))
  }

  test("multiple chunk output at boundary size") {
    val ref = new Array[Byte](30)
    Random.nextBytes(ref)
    val o = new ChunkedByteBufferOutputStream(10, ByteBuffer.allocate)
    o.write(ref)
    o.close()
    val arrays = o.toChunkedByteBuffer.getChunks().map(_.array())
    assert(arrays.length === 3)
    assert(arrays(0).length === 10)
    assert(arrays(1).length === 10)
    assert(arrays(2).length === 10)

    assert(arrays(0).toSeq === ref.slice(0, 10))
    assert(arrays(1).toSeq === ref.slice(10, 20))
    assert(arrays(2).toSeq === ref.slice(20, 30))
  }
}
