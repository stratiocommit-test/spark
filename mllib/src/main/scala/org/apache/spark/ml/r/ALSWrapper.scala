/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class ALSWrapper private (
    val alsModel: ALSModel,
    val ratingCol: String) extends MLWritable {

  lazy val userCol: String = alsModel.getUserCol
  lazy val itemCol: String = alsModel.getItemCol
  lazy val userFactors: DataFrame = alsModel.userFactors
  lazy val itemFactors: DataFrame = alsModel.itemFactors
  lazy val rank: Int = alsModel.rank

  def transform(dataset: Dataset[_]): DataFrame = {
    alsModel.transform(dataset)
  }

  override def write: MLWriter = new ALSWrapper.ALSWrapperWriter(this)
}

private[r] object ALSWrapper extends MLReadable[ALSWrapper] {

  def fit(  // scalastyle:ignore
      data: DataFrame,
      ratingCol: String,
      userCol: String,
      itemCol: String,
      rank: Int,
      regParam: Double,
      maxIter: Int,
      implicitPrefs: Boolean,
      alpha: Double,
      nonnegative: Boolean,
      numUserBlocks: Int,
      numItemBlocks: Int,
      checkpointInterval: Int,
      seed: Int): ALSWrapper = {

    val als = new ALS()
      .setRatingCol(ratingCol)
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setImplicitPrefs(implicitPrefs)
      .setAlpha(alpha)
      .setNonnegative(nonnegative)
      .setNumBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
      .setCheckpointInterval(checkpointInterval)
      .setSeed(seed.toLong)

    val alsModel: ALSModel = als.fit(data)

    new ALSWrapper(alsModel, ratingCol)
  }

  override def read: MLReader[ALSWrapper] = new ALSWrapperReader

  override def load(path: String): ALSWrapper = super.load(path)

  class ALSWrapperWriter(instance: ALSWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("ratingCol" -> instance.ratingCol)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.alsModel.save(modelPath)
    }
  }

  class ALSWrapperReader extends MLReader[ALSWrapper] {

    override def load(path: String): ALSWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val ratingCol = (rMetadata \ "ratingCol").extract[String]
      val alsModel = ALSModel.load(modelPath)

      new ALSWrapper(alsModel, ratingCol)
    }
  }

}
