/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.api.python

import scala.collection.JavaConverters

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.linalg.Matrix

/**
 * Wrapper around LDAModel to provide helper methods in Python
 */
private[python] class LDAModelWrapper(model: LDAModel) {

  def topicsMatrix(): Matrix = model.topicsMatrix

  def vocabSize(): Int = model.vocabSize

  def describeTopics(): Array[Byte] = describeTopics(this.model.vocabSize)

  def describeTopics(maxTermsPerTopic: Int): Array[Byte] = {
    val topics = model.describeTopics(maxTermsPerTopic).map { case (terms, termWeights) =>
      val jTerms = JavaConverters.seqAsJavaListConverter(terms).asJava
      val jTermWeights = JavaConverters.seqAsJavaListConverter(termWeights).asJava
      Array[Any](jTerms, jTermWeights)
    }
    SerDe.dumps(JavaConverters.seqAsJavaListConverter(topics).asJava)
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
