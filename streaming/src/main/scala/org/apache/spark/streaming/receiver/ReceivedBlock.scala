/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

/** Trait representing a received block */
private[streaming] sealed trait ReceivedBlock

/** class representing a block received as an ArrayBuffer */
private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock

/** class representing a block received as an Iterator */
private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock

/** class representing a block received as a ByteBuffer */
private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) extends ReceivedBlock
