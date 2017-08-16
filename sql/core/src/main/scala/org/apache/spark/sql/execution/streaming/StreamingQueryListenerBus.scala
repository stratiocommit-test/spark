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

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[StreamingQueryListener]]s. This one will send received
 * [[StreamingQueryListener.Event]]s to the Spark listener bus. It also registers itself with
 * Spark listener bus, so that it can receive [[StreamingQueryListener.Event]]s and dispatch them
 * to StreamingQueryListeners.
 *
 * Note that each bus and its registered listeners are associated with a single SparkSession
 * and StreamingQueryManager. So this bus will dispatch events to registered listeners for only
 * those queries that were started in the associated SparkSession.
 */
class StreamingQueryListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingQueryListener, StreamingQueryListener.Event] {

  import StreamingQueryListener._

  sparkListenerBus.addListener(this)

  /**
   * RunIds of active queries whose events are supposed to be forwarded by this ListenerBus
   * to registered `StreamingQueryListeners`.
   *
   * Note 1: We need to track runIds instead of ids because the runId is unique for every started
   * query, even it its a restart. So even if a query is restarted, this bus will identify them
   * separately and correctly account for the restart.
   *
   * Note 2: This list needs to be maintained separately from the
   * `StreamingQueryManager.activeQueries` because a terminated query is cleared from
   * `StreamingQueryManager.activeQueries` as soon as it is stopped, but the this ListenerBus
   * must clear a query only after the termination event of that query has been posted.
   */
  private val activeQueryRunIds = new mutable.HashSet[UUID]

  /**
   * Post a StreamingQueryListener event to the added StreamingQueryListeners.
   * Note that only the QueryStarted event is posted to the listener synchronously. Other events
   * are dispatched to Spark listener bus. This method is guaranteed to be called by queries in
   * the same SparkSession as this listener.
   */
  def post(event: StreamingQueryListener.Event) {
    event match {
      case s: QueryStartedEvent =>
        activeQueryRunIds.synchronized { activeQueryRunIds += s.runId }
        sparkListenerBus.post(s)
        // post to local listeners to trigger callbacks
        postToAll(s)
      case _ =>
        sparkListenerBus.post(event)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingQueryListener.Event =>
        // SPARK-18144: we broadcast QueryStartedEvent to all listeners attached to this bus
        // synchronously and the ones attached to LiveListenerBus asynchronously. Therefore,
        // we need to ignore QueryStartedEvent if this method is called within SparkListenerBus
        // thread
        if (!LiveListenerBus.withinListenerThread.value || !e.isInstanceOf[QueryStartedEvent]) {
          postToAll(e)
        }
      case _ =>
    }
  }

  /**
   * Dispatch events to registered StreamingQueryListeners. Only the events associated queries
   * started in the same SparkSession as this ListenerBus will be dispatched to the listeners.
   */
  override protected def doPostEvent(
      listener: StreamingQueryListener,
      event: StreamingQueryListener.Event): Unit = {
    def shouldReport(runId: UUID): Boolean = {
      activeQueryRunIds.synchronized { activeQueryRunIds.contains(runId) }
    }

    event match {
      case queryStarted: QueryStartedEvent =>
        if (shouldReport(queryStarted.runId)) {
          listener.onQueryStarted(queryStarted)
        }
      case queryProgress: QueryProgressEvent =>
        if (shouldReport(queryProgress.progress.runId)) {
          listener.onQueryProgress(queryProgress)
        }
      case queryTerminated: QueryTerminatedEvent =>
        if (shouldReport(queryTerminated.runId)) {
          listener.onQueryTerminated(queryTerminated)
          activeQueryRunIds.synchronized { activeQueryRunIds -= queryTerminated.runId }
        }
      case _ =>
    }
  }
}
