/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.pmml.export

import scala.{Array => SArray}

import org.dmg.pmml._

import org.apache.spark.mllib.clustering.KMeansModel

/**
 * PMML Model Export for KMeansModel class
 */
private[mllib] class KMeansPMMLModelExport(model: KMeansModel) extends PMMLModelExport{

  populateKMeansPMML(model)

  /**
   * Export the input KMeansModel model to PMML format.
   */
  private def populateKMeansPMML(model: KMeansModel): Unit = {
    pmml.getHeader.setDescription("k-means clustering")

    if (model.clusterCenters.length > 0) {
      val clusterCenter = model.clusterCenters(0)
      val fields = new SArray[FieldName](clusterCenter.size)
      val dataDictionary = new DataDictionary
      val miningSchema = new MiningSchema
      val comparisonMeasure = new ComparisonMeasure()
        .setKind(ComparisonMeasure.Kind.DISTANCE)
        .setMeasure(new SquaredEuclidean())
      val clusteringModel = new ClusteringModel()
        .setModelName("k-means")
        .setMiningSchema(miningSchema)
        .setComparisonMeasure(comparisonMeasure)
        .setFunctionName(MiningFunctionType.CLUSTERING)
        .setModelClass(ClusteringModel.ModelClass.CENTER_BASED)
        .setNumberOfClusters(model.clusterCenters.length)

      for (i <- 0 until clusterCenter.size) {
        fields(i) = FieldName.create("field_" + i)
        dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
        miningSchema
          .addMiningFields(new MiningField(fields(i))
          .setUsageType(FieldUsageType.ACTIVE))
        clusteringModel.addClusteringFields(
          new ClusteringField(fields(i)).setCompareFunction(CompareFunctionType.ABS_DIFF))
      }

      dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

      for (i <- model.clusterCenters.indices) {
        val cluster = new Cluster()
          .setName("cluster_" + i)
          .setArray(new org.dmg.pmml.Array()
          .setType(Array.Type.REAL)
          .setN(clusterCenter.size)
          .setValue(model.clusterCenters(i).toArray.mkString(" ")))
        // we don't have the size of the single cluster but only the centroids (withValue)
        // .withSize(value)
        clusteringModel.addClusters(cluster)
      }

      pmml.setDataDictionary(dataDictionary)
      pmml.addModels(clusteringModel)
    }
  }
}
