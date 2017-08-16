/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.ui

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, SparkUITab}

class SQLTab(val listener: SQLListener, sparkUI: SparkUI)
  extends SparkUITab(sparkUI, "SQL") with Logging {

  val parent = sparkUI

  attachPage(new AllExecutionsPage(this))
  attachPage(new ExecutionPage(this))
  parent.attachTab(this)

  parent.addStaticHandler(SQLTab.STATIC_RESOURCE_DIR, "/static/sql")
}

object SQLTab {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/sql/execution/ui/static"
}
