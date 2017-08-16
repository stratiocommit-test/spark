/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.pmml

import java.io.{File, OutputStream, StringWriter}
import javax.xml.transform.stream.StreamResult

import org.jpmml.model.JAXBUtil

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.pmml.export.PMMLModelExportFactory

/**
 * :: DeveloperApi ::
 * Export model to the PMML format
 * Predictive Model Markup Language (PMML) is an XML-based file format
 * developed by the Data Mining Group (www.dmg.org).
 */
@DeveloperApi
@Since("1.4.0")
trait PMMLExportable {

  /**
   * Export the model to the stream result in PMML format
   */
  private def toPMML(streamResult: StreamResult): Unit = {
    val pmmlModelExport = PMMLModelExportFactory.createPMMLModelExport(this)
    JAXBUtil.marshalPMML(pmmlModelExport.getPmml, streamResult)
  }

  /**
   * Export the model to a local file in PMML format
   */
  @Since("1.4.0")
  def toPMML(localPath: String): Unit = {
    toPMML(new StreamResult(new File(localPath)))
  }

  /**
   * Export the model to a directory on a distributed file system in PMML format
   */
  @Since("1.4.0")
  def toPMML(sc: SparkContext, path: String): Unit = {
    val pmml = toPMML()
    sc.parallelize(Array(pmml), 1).saveAsTextFile(path)
  }

  /**
   * Export the model to the OutputStream in PMML format
   */
  @Since("1.4.0")
  def toPMML(outputStream: OutputStream): Unit = {
    toPMML(new StreamResult(outputStream))
  }

  /**
   * Export the model to a String in PMML format
   */
  @Since("1.4.0")
  def toPMML(): String = {
    val writer = new StringWriter
    toPMML(new StreamResult(writer))
    writer.toString
  }

}
