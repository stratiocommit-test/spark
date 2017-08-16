/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler

import collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi

// information about a specific split instance : handles both split instances.
// So that we do not need to worry about the differences.
@DeveloperApi
class SplitInfo(
    val inputFormatClazz: Class[_],
    val hostLocation: String,
    val path: String,
    val length: Long,
    val underlyingSplit: Any) {
  override def toString(): String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz +
      ", hostLocation : " + hostLocation + ", path : " + path +
      ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    // ignore overflow ? It is hashcode anyway !
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    hashCode
  }

  // This is practically useless since most of the Split impl's don't seem to implement equals :-(
  // So unless there is identity equality between underlyingSplits, it will always fail even if it
  // is pointing to same block.
  override def equals(other: Any): Boolean = other match {
    case that: SplitInfo =>
      this.hostLocation == that.hostLocation &&
        this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path &&
        this.length == that.length &&
        // other split specific checks (like start for FileSplit)
        this.underlyingSplit == that.underlyingSplit
    case _ => false
  }
}

object SplitInfo {

  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapredSplit: org.apache.hadoop.mapred.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    retval
  }

  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    retval
  }
}
