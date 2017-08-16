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

import org.apache.spark.mllib.regression.GeneralizedLinearModel

/**
 * PMML Model Export for GeneralizedLinearModel class with binary ClassificationModel
 */
private[mllib] class BinaryClassificationPMMLModelExport(
    model: GeneralizedLinearModel,
    description: String,
    normalizationMethod: RegressionNormalizationMethodType,
    threshold: Double)
  extends PMMLModelExport {

  populateBinaryClassificationPMML()

  /**
   * Export the input LogisticRegressionModel or SVMModel to PMML format.
   */
  private def populateBinaryClassificationPMML(): Unit = {
     pmml.getHeader.setDescription(description)

     if (model.weights.size > 0) {
       val fields = new SArray[FieldName](model.weights.size)
       val dataDictionary = new DataDictionary
       val miningSchema = new MiningSchema
       val regressionTableYES = new RegressionTable(model.intercept).setTargetCategory("1")
       var interceptNO = threshold
       if (RegressionNormalizationMethodType.LOGIT == normalizationMethod) {
         if (threshold <= 0) {
           interceptNO = Double.MinValue
         } else if (threshold >= 1) {
           interceptNO = Double.MaxValue
         } else {
           interceptNO = -math.log(1 / threshold - 1)
         }
       }
       val regressionTableNO = new RegressionTable(interceptNO).setTargetCategory("0")
       val regressionModel = new RegressionModel()
         .setFunctionName(MiningFunctionType.CLASSIFICATION)
         .setMiningSchema(miningSchema)
         .setModelName(description)
         .setNormalizationMethod(normalizationMethod)
         .addRegressionTables(regressionTableYES, regressionTableNO)

       for (i <- 0 until model.weights.size) {
         fields(i) = FieldName.create("field_" + i)
         dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
         miningSchema
           .addMiningFields(new MiningField(fields(i))
           .setUsageType(FieldUsageType.ACTIVE))
         regressionTableYES.addNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))
       }

       // add target field
       val targetField = FieldName.create("target")
       dataDictionary
         .addDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.STRING))
       miningSchema
         .addMiningFields(new MiningField(targetField)
         .setUsageType(FieldUsageType.TARGET))

       dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

       pmml.setDataDictionary(dataDictionary)
       pmml.addModels(regressionModel)
     }
  }
}
