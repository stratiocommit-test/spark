/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.util

import java.util.concurrent.atomic.AtomicLong

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.Param
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
 * A small wrapper that defines a training session for an estimator, and some methods to log
 * useful information during this session.
 *
 * A new instance is expected to be created within fit().
 *
 * @param estimator the estimator that is being fit
 * @param dataset the training dataset
 * @tparam E the type of the estimator
 */
private[spark] class Instrumentation[E <: Estimator[_]] private (
    estimator: E, dataset: RDD[_]) extends Logging {

  private val id = Instrumentation.counter.incrementAndGet()
  private val prefix = {
    val className = estimator.getClass.getSimpleName
    s"$className-${estimator.uid}-${dataset.hashCode()}-$id: "
  }

  init()

  private def init(): Unit = {
    log(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  /**
   * Logs a message with a prefix that uniquely identifies the training session.
   */
  def log(msg: String): Unit = {
    logInfo(prefix + msg)
  }

  /**
   * Logs the value of the given parameters for the estimator being used in this session.
   */
  def logParams(params: Param[_]*): Unit = {
    val pairs: Seq[(String, JValue)] = for {
      p <- params
      value <- estimator.get(p)
    } yield {
      val cast = p.asInstanceOf[Param[Any]]
      p.name -> parse(cast.jsonEncode(value))
    }
    log(compact(render(map2jvalue(pairs.toMap))))
  }

  def logNumFeatures(num: Long): Unit = {
    log(compact(render("numFeatures" -> num)))
  }

  def logNumClasses(num: Long): Unit = {
    log(compact(render("numClasses" -> num)))
  }

  /**
   * Logs the value with customized name field.
   */
  def logNamedValue(name: String, num: Long): Unit = {
    log(compact(render(name -> num)))
  }

  /**
   * Logs the successful completion of the training session and the value of the learned model.
   */
  def logSuccess(model: Model[_]): Unit = {
    log(s"training finished")
  }
}

/**
 * Some common methods for logging information about a training session.
 */
private[spark] object Instrumentation {
  private val counter = new AtomicLong(0)

  /**
   * Creates an instrumentation object for a training session.
   */
  def create[E <: Estimator[_]](
      estimator: E, dataset: Dataset[_]): Instrumentation[E] = {
    create[E](estimator, dataset.rdd)
  }

  /**
   * Creates an instrumentation object for a training session.
   */
  def create[E <: Estimator[_]](
      estimator: E, dataset: RDD[_]): Instrumentation[E] = {
    new Instrumentation[E](estimator, dataset)
  }

}
