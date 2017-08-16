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

import org.dmg.pmml.ClusteringModel

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

class KMeansPMMLModelExportSuite extends SparkFunSuite {

  test("KMeansPMMLModelExport generate PMML format") {
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0))
    val kmeansModel = new KMeansModel(clusterCenters)

    val modelExport = PMMLModelExportFactory.createPMMLModelExport(kmeansModel)

    // assert that the PMML format is as expected
    assert(modelExport.isInstanceOf[PMMLModelExport])
    val pmml = modelExport.asInstanceOf[PMMLModelExport].getPmml
    assert(pmml.getHeader.getDescription === "k-means clustering")
    // check that the number of fields match the single vector size
    assert(pmml.getDataDictionary.getNumberOfFields === clusterCenters(0).size)
    // This verify that there is a model attached to the pmml object and the model is a clustering
    // one. It also verifies that the pmml model has the same number of clusters of the spark model.
    val pmmlClusteringModel = pmml.getModels.get(0).asInstanceOf[ClusteringModel]
    assert(pmmlClusteringModel.getNumberOfClusters === clusterCenters.length)
  }

}
