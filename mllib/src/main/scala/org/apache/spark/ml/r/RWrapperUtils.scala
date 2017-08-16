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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.feature.{RFormula, RFormulaModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

private[r] object RWrapperUtils extends Logging {

  /**
   * DataFrame column check.
   * When loading libsvm data, default columns "features" and "label" will be added.
   * And "features" would conflict with RFormula default feature column names.
   * Here is to change the column name to avoid "column already exists" error.
   *
   * @param rFormula RFormula instance
   * @param data Input dataset
   */
  def checkDataColumns(rFormula: RFormula, data: Dataset[_]): Unit = {
    if (data.schema.fieldNames.contains(rFormula.getFeaturesCol)) {
      val newFeaturesName = s"${Identifiable.randomUID(rFormula.getFeaturesCol)}"
      logInfo(s"data containing ${rFormula.getFeaturesCol} column, " +
        s"using new name $newFeaturesName instead")
      rFormula.setFeaturesCol(newFeaturesName)
    }

    if (rFormula.getForceIndexLabel && data.schema.fieldNames.contains(rFormula.getLabelCol)) {
      val newLabelName = s"${Identifiable.randomUID(rFormula.getLabelCol)}"
      logInfo(s"data containing ${rFormula.getLabelCol} column and we force to index label, " +
        s"using new name $newLabelName instead")
      rFormula.setLabelCol(newLabelName)
    }
  }

  /**
   * Get the feature names and original labels from the schema
   * of DataFrame transformed by RFormulaModel.
   *
   * @param rFormulaModel The RFormulaModel instance.
   * @param data Input dataset.
   * @return The feature names and original labels.
   */
  def getFeaturesAndLabels(
      rFormulaModel: RFormulaModel,
      data: Dataset[_]): (Array[String], Array[String]) = {
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    val labelAttr = Attribute.fromStructField(schema(rFormulaModel.getLabelCol))
      .asInstanceOf[NominalAttribute]
    val labels = labelAttr.values.get
    (features, labels)
  }
}
