/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg

import org.apache.spark.annotation.Since

/**
 * Represents singular value decomposition (SVD) factors.
 */
@Since("1.0.0")
case class SingularValueDecomposition[UType, VType](U: UType, s: Vector, V: VType)

/**
 * Represents QR factors.
 */
@Since("1.5.0")
case class QRDecomposition[QType, RType](Q: QType, R: RType)

