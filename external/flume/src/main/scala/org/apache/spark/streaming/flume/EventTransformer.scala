/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.flume

import java.io.{ObjectInput, ObjectOutput}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A simple object that provides the implementation of readExternal and writeExternal for both
 * the wrapper classes for Flume-style Events.
 */
private[streaming] object EventTransformer extends Logging {
  def readExternal(in: ObjectInput): (java.util.HashMap[CharSequence, CharSequence],
    Array[Byte]) = {
    val bodyLength = in.readInt()
    val bodyBuff = new Array[Byte](bodyLength)
    in.readFully(bodyBuff)

    val numHeaders = in.readInt()
    val headers = new java.util.HashMap[CharSequence, CharSequence]

    for (i <- 0 until numHeaders) {
      val keyLength = in.readInt()
      val keyBuff = new Array[Byte](keyLength)
      in.readFully(keyBuff)
      val key: String = Utils.deserialize(keyBuff)

      val valLength = in.readInt()
      val valBuff = new Array[Byte](valLength)
      in.readFully(valBuff)
      val value: String = Utils.deserialize(valBuff)

      headers.put(key, value)
    }
    (headers, bodyBuff)
  }

  def writeExternal(out: ObjectOutput, headers: java.util.Map[CharSequence, CharSequence],
    body: Array[Byte]) {
    out.writeInt(body.length)
    out.write(body)
    val numHeaders = headers.size()
    out.writeInt(numHeaders)
    for ((k, v) <- headers.asScala) {
      val keyBuff = Utils.serialize(k.toString)
      out.writeInt(keyBuff.length)
      out.write(keyBuff)
      val valBuff = Utils.serialize(v.toString)
      out.writeInt(valBuff.length)
      out.write(valBuff)
    }
  }
}
