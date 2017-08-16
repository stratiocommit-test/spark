/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.pmml.export

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.beans.BeanProperty

import org.dmg.pmml.{Application, Header, PMML, Timestamp}

private[mllib] trait PMMLModelExport {

  /**
   * Holder of the exported model in PMML format
   */
  @BeanProperty
  val pmml: PMML = {
    val version = getClass.getPackage.getImplementationVersion
    val app = new Application("Apache Spark MLlib").setVersion(version)
    val timestamp = new Timestamp()
      .addContent(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).format(new Date()))
    val header = new Header()
      .setApplication(app)
      .setTimestamp(timestamp)
    new PMML("4.2", header, null)
  }
}
