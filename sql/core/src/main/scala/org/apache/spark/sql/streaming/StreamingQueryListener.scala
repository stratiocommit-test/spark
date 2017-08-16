/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.annotation.Experimental
import org.apache.spark.scheduler.SparkListenerEvent

/**
 * :: Experimental ::
 * Interface for listening to events related to [[StreamingQuery StreamingQueries]].
 * @note The methods are not thread-safe as they may be called from different threads.
 *
 * @since 2.0.0
 */
@Experimental
abstract class StreamingQueryListener {

  import StreamingQueryListener._

  /**
   * Called when a query is started.
   * @note This is called synchronously with
   *       [[org.apache.spark.sql.streaming.DataStreamWriter `DataStreamWriter.start()`]],
   *       that is, `onQueryStart` will be called on all listeners before
   *       `DataStreamWriter.start()` returns the corresponding [[StreamingQuery]]. Please
   *       don't block this method as it will block your query.
   * @since 2.0.0
   */
  def onQueryStarted(event: QueryStartedEvent): Unit

  /**
   * Called when there is some status update (ingestion rate updated, etc.)
   *
   * @note This method is asynchronous. The status in [[StreamingQuery]] will always be
   *       latest no matter when this method is called. Therefore, the status of [[StreamingQuery]]
   *       may be changed before/when you process the event. E.g., you may find [[StreamingQuery]]
   *       is terminated when you are processing [[QueryProgressEvent]].
   * @since 2.0.0
   */
  def onQueryProgress(event: QueryProgressEvent): Unit

  /**
   * Called when a query is stopped, with or without error.
   * @since 2.0.0
   */
  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}


/**
 * :: Experimental ::
 * Companion object of [[StreamingQueryListener]] that defines the listener events.
 * @since 2.0.0
 */
@Experimental
object StreamingQueryListener {

  /**
   * :: Experimental ::
   * Base type of [[StreamingQueryListener]] events
   * @since 2.0.0
   */
  @Experimental
  trait Event extends SparkListenerEvent

  /**
   * :: Experimental ::
   * Event representing the start of a query
   * @param id An unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param name User-specified name of the query, null if not specified.
   * @since 2.1.0
   */
  @Experimental
  class QueryStartedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val name: String) extends Event

  /**
   * :: Experimental ::
   * Event representing any progress updates in a query.
   * @param progress The query progress updates.
   * @since 2.1.0
   */
  @Experimental
  class QueryProgressEvent private[sql](val progress: StreamingQueryProgress) extends Event

  /**
   * :: Experimental ::
   * Event representing that termination of a query.
   *
   * @param id An unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param exception The exception message of the query if the query was terminated
   *                  with an exception. Otherwise, it will be `None`.
   * @since 2.1.0
   */
  @Experimental
  class QueryTerminatedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val exception: Option[String]) extends Event
}
