/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.ui

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * Spark Web UI tab that shows statistics of a streaming job.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
private[spark] class StreamingTab(val ssc: StreamingContext)
  extends SparkUITab(StreamingTab.getSparkUI(ssc), "streaming") with Logging {

  import StreamingTab._

  private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"

  val parent = getSparkUI(ssc)
  val listener = ssc.progressListener

  ssc.addStreamingListener(listener)
  ssc.sc.addSparkListener(listener)
  attachPage(new StreamingPage(this))
  attachPage(new BatchPage(this))

  def attach() {
    getSparkUI(ssc).attachTab(this)
    getSparkUI(ssc).addStaticHandler(STATIC_RESOURCE_DIR, "/static/streaming")
  }

  def detach() {
    getSparkUI(ssc).detachTab(this)
    getSparkUI(ssc).removeStaticHandler("/static/streaming")
  }
}

private object StreamingTab {
  def getSparkUI(ssc: StreamingContext): SparkUI = {
    ssc.sc.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
