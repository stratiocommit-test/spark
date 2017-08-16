/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.impl

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel


/**
 * This class helps with persisting and checkpointing Graphs.
 * Specifically, it automatically handles persisting and (optionally) checkpointing, as well as
 * unpersisting and removing checkpoint files.
 *
 * Users should call update() when a new graph has been created,
 * before the graph has been materialized.  After updating [[PeriodicGraphCheckpointer]], users are
 * responsible for materializing the graph to ensure that persisting and checkpointing actually
 * occur.
 *
 * When update() is called, this does the following:
 *  - Persist new graph (if not yet persisted), and put in queue of persisted graphs.
 *  - Unpersist graphs from queue until there are at most 3 persisted graphs.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new graph, and put in a queue of checkpointed graphs.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which Graphs should be
 *    checkpointed).
 *  - This class removes checkpoint files once later graphs have been checkpointed.
 *    However, references to the older graphs will still return isCheckpointed = true.
 *
 * Example usage:
 * {{{
 *  val (graph1, graph2, graph3, ...) = ...
 *  val cp = new PeriodicGraphCheckpointer(2, sc)
 *  graph1.vertices.count(); graph1.edges.count()
 *  // persisted: graph1
 *  cp.updateGraph(graph2)
 *  graph2.vertices.count(); graph2.edges.count()
 *  // persisted: graph1, graph2
 *  // checkpointed: graph2
 *  cp.updateGraph(graph3)
 *  graph3.vertices.count(); graph3.edges.count()
 *  // persisted: graph1, graph2, graph3
 *  // checkpointed: graph2
 *  cp.updateGraph(graph4)
 *  graph4.vertices.count(); graph4.edges.count()
 *  // persisted: graph2, graph3, graph4
 *  // checkpointed: graph4
 *  cp.updateGraph(graph5)
 *  graph5.vertices.count(); graph5.edges.count()
 *  // persisted: graph3, graph4, graph5
 *  // checkpointed: graph4
 * }}}
 *
 * @param checkpointInterval Graphs will be checkpointed at this interval.
 *                           If this interval was set as -1, then checkpointing will be disabled.
 * @tparam VD  Vertex descriptor type
 * @tparam ED  Edge descriptor type
 *
 * TODO: Move this out of MLlib?
 */
private[mllib] class PeriodicGraphCheckpointer[VD, ED](
    checkpointInterval: Int,
    sc: SparkContext)
  extends PeriodicCheckpointer[Graph[VD, ED]](checkpointInterval, sc) {

  override protected def checkpoint(data: Graph[VD, ED]): Unit = data.checkpoint()

  override protected def isCheckpointed(data: Graph[VD, ED]): Boolean = data.isCheckpointed

  override protected def persist(data: Graph[VD, ED]): Unit = {
    if (data.vertices.getStorageLevel == StorageLevel.NONE) {
      data.vertices.persist()
    }
    if (data.edges.getStorageLevel == StorageLevel.NONE) {
      data.edges.persist()
    }
  }

  override protected def unpersist(data: Graph[VD, ED]): Unit = data.unpersist(blocking = false)

  override protected def getCheckpointFiles(data: Graph[VD, ED]): Iterable[String] = {
    data.getCheckpointFiles
  }
}
