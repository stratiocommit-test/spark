/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

/**
 * Extractor Object for pulling out the root cause of an error.
 * If the error contains no cause, it will return the error itself.
 *
 * Usage:
 * try {
 *   ...
 * } catch {
 *   case CausedBy(ex: CommitDeniedException) => ...
 * }
 */
private[spark] object CausedBy {

  def unapply(e: Throwable): Option[Throwable] = {
    Option(e.getCause).flatMap(cause => unapply(cause)).orElse(Some(e))
  }
}
