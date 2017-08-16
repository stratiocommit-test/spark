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

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * Information about an [[org.apache.spark.Accumulable]] modified during a task or stage.
 *
 * @param id accumulator ID
 * @param name accumulator name
 * @param update partial value from a task, may be None if used on driver to describe a stage
 * @param value total accumulated value so far, maybe None if used on executors to describe a task
 * @param internal whether this accumulator was internal
 * @param countFailedValues whether to count this accumulator's partial value if the task failed
 * @param metadata internal metadata associated with this accumulator, if any
 *
 * @note Once this is JSON serialized the types of `update` and `value` will be lost and be
 * cast to strings. This is because the user can define an accumulator of any type and it will
 * be difficult to preserve the type in consumers of the event log. This does not apply to
 * internal accumulators that represent task level metrics.
 */
@DeveloperApi
case class AccumulableInfo private[spark] (
    id: Long,
    name: Option[String],
    update: Option[Any], // represents a partial update within a task
    value: Option[Any],
    private[spark] val internal: Boolean,
    private[spark] val countFailedValues: Boolean,
    // TODO: use this to identify internal task metrics instead of encoding it in the name
    private[spark] val metadata: Option[String] = None)


/**
 * A collection of deprecated constructors. This will be removed soon.
 */
object AccumulableInfo {

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(
      id: Long,
      name: String,
      update: Option[String],
      value: String,
      internal: Boolean): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), update, Option(value), internal, countFailedValues = false)
  }

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(id: Long, name: String, update: Option[String], value: String): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), update, Option(value), internal = false, countFailedValues = false)
  }

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(id: Long, name: String, value: String): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), None, Option(value), internal = false, countFailedValues = false)
  }
}
