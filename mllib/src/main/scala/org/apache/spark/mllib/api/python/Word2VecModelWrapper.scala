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

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Wrapper around Word2VecModel to provide helper methods in Python
 */
private[python] class Word2VecModelWrapper(model: Word2VecModel) {
  def transform(word: String): Vector = {
    model.transform(word)
  }

  /**
   * Transforms an RDD of words to its vector representation
   * @param rdd an RDD of words
   * @return an RDD of vector representations of words
   */
  def transform(rdd: JavaRDD[String]): JavaRDD[Vector] = {
    rdd.rdd.map(model.transform)
  }

  /**
   * Finds synonyms of a word; do not include the word itself in results.
   * @param word a word
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(word: String, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(word, num))
  }

  /**
   * Finds words similar to the the vector representation of a word without
   * filtering results.
   * @param vector a vector
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(vector: Vector, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(vector, num))
  }

  private def prepareResult(result: Array[(String, Double)]) = {
    val similarity = Vectors.dense(result.map(_._2))
    val words = result.map(_._1)
    List(words, similarity).map(_.asInstanceOf[Object]).asJava
  }


  def getVectors: JMap[String, JList[Float]] = {
    model.getVectors.map { case (k, v) =>
      (k, v.toList.asJava)
    }.asJava
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
