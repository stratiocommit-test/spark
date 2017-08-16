/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.streaming

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType

package object state {

  implicit class StateStoreOps[T: ClassTag](dataRDD: RDD[T]) {

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    def mapPartitionsWithStateStore[U: ClassTag](
        sqlContext: SQLContext,
        checkpointLocation: String,
        operatorId: Long,
        storeVersion: Long,
        keySchema: StructType,
        valueSchema: StructType)(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      mapPartitionsWithStateStore(
        checkpointLocation,
        operatorId,
        storeVersion,
        keySchema,
        valueSchema,
        sqlContext.sessionState,
        Some(sqlContext.streams.stateStoreCoordinator))(
        storeUpdateFunction)
    }

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    private[streaming] def mapPartitionsWithStateStore[U: ClassTag](
        checkpointLocation: String,
        operatorId: Long,
        storeVersion: Long,
        keySchema: StructType,
        valueSchema: StructType,
        sessionState: SessionState,
        storeCoordinator: Option[StateStoreCoordinatorRef])(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {
      val cleanedF = dataRDD.sparkContext.clean(storeUpdateFunction)
      new StateStoreRDD(
        dataRDD,
        cleanedF,
        checkpointLocation,
        operatorId,
        storeVersion,
        keySchema,
        valueSchema,
        sessionState,
        storeCoordinator)
    }
  }
}
