/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.linalg

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.types.DataType

/**
 * :: DeveloperApi ::
 * SQL data types for vectors and matrices.
 */
@Since("2.0.0")
@DeveloperApi
object SQLDataTypes {

  /** Data type for [[Vector]]. */
  val VectorType: DataType = new VectorUDT

  /** Data type for [[Matrix]]. */
  val MatrixType: DataType = new MatrixUDT
}
