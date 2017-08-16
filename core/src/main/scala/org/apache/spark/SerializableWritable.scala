/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

@DeveloperApi
class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {

  def value: T = t

  override def toString: String = t.toString

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new Configuration(false))
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
