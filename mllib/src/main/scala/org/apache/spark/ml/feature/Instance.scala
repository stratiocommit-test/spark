/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector

/**
 * Class that represents an instance of weighted data point with label and features.
 *
 * @param label Label for this data point.
 * @param weight The weight of this instance.
 * @param features The vector of features for this data point.
 */
private[ml] case class Instance(label: Double, weight: Double, features: Vector)
