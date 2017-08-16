/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui.env

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.ui._

private[ui] class EnvironmentTab(parent: SparkUI) extends SparkUITab(parent, "environment") {
  val listener = parent.environmentListener
  attachPage(new EnvironmentPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the EnvironmentTab
 */
@DeveloperApi
class EnvironmentListener extends SparkListener {
  var jvmInformation = Seq[(String, String)]()
  var sparkProperties = Seq[(String, String)]()
  var systemProperties = Seq[(String, String)]()
  var classpathEntries = Seq[(String, String)]()

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      val environmentDetails = environmentUpdate.environmentDetails
      jvmInformation = environmentDetails("JVM Information")
      sparkProperties = environmentDetails("Spark Properties")
      systemProperties = environmentDetails("System Properties")
      classpathEntries = environmentDetails("Classpath Entries")
    }
  }
}
