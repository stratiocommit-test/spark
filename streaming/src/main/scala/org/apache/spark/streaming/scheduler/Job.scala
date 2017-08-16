/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import scala.util.{Failure, Try}

import org.apache.spark.streaming.Time
import org.apache.spark.util.{CallSite, Utils}

/**
 * Class representing a Spark computation. It may contain multiple Spark jobs.
 */
private[streaming]
class Job(val time: Time, func: () => _) {
  private var _id: String = _
  private var _outputOpId: Int = _
  private var isSet = false
  private var _result: Try[_] = null
  private var _callSite: CallSite = null
  private var _startTime: Option[Long] = None
  private var _endTime: Option[Long] = None

  def run() {
    _result = Try(func())
  }

  def result: Try[_] = {
    if (_result == null) {
      throw new IllegalStateException("Cannot access result before job finishes")
    }
    _result
  }

  /**
   * @return the global unique id of this Job.
   */
  def id: String = {
    if (!isSet) {
      throw new IllegalStateException("Cannot access id before calling setId")
    }
    _id
  }

  /**
   * @return the output op id of this Job. Each Job has a unique output op id in the same JobSet.
   */
  def outputOpId: Int = {
    if (!isSet) {
      throw new IllegalStateException("Cannot access number before calling setId")
    }
    _outputOpId
  }

  def setOutputOpId(outputOpId: Int) {
    if (isSet) {
      throw new IllegalStateException("Cannot call setOutputOpId more than once")
    }
    isSet = true
    _id = s"streaming job $time.$outputOpId"
    _outputOpId = outputOpId
  }

  def setCallSite(callSite: CallSite): Unit = {
    _callSite = callSite
  }

  def callSite: CallSite = _callSite

  def setStartTime(startTime: Long): Unit = {
    _startTime = Some(startTime)
  }

  def setEndTime(endTime: Long): Unit = {
    _endTime = Some(endTime)
  }

  def toOutputOperationInfo: OutputOperationInfo = {
    val failureReason = if (_result != null && _result.isFailure) {
      Some(Utils.exceptionString(_result.asInstanceOf[Failure[_]].exception))
    } else {
      None
    }
    OutputOperationInfo(
      time, outputOpId, callSite.shortForm, callSite.longForm, _startTime, _endTime, failureReason)
  }

  override def toString: String = id
}
