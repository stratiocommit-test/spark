/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.security

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermissions

import org.apache.spark.internal.Logging

object HDFSConfig extends Logging{

  private def downloadFile(url: String, fileToDownload: String, outputhPath: String) {
    logInfo(s"extracting $url/$fileToDownload")
    new File(outputhPath).mkdirs
    val src = scala.io.Source.fromURL(s"$url/$fileToDownload")
    val downloadFile = Files.createFile(Paths.get(s"$outputhPath/$fileToDownload"),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    downloadFile.toFile.deleteOnExit() // just to be sure
    Files.write(downloadFile, src.mkString("").getBytes)
  }

  def prepareEnviroment(options: Map[String, String]): Map[String, String] = {
    require(options.get("HDFS_CONF_URI").isDefined,
      "a proper HDFS URI must be configured to get Hadoop Configuration")
    require(sys.env.get("HADOOP_CONF_DIR").isDefined,
      "a proper Hadoop Conf Dir must be configured to store Hadoop Configuration")
    val hadoopConfUri = options.get("HDFS_CONF_URI").get
    val hadoopConfDir = sys.env.get("HADOOP_CONF_DIR").get
    downloadFile(hadoopConfUri, "core-site.xml", hadoopConfDir)
    downloadFile(hadoopConfUri, "hdfs-site.xml", hadoopConfDir)
    downloadFile(hadoopConfUri, "krb5.conf", "/etc/")
    Map.empty[String, String]
  }
}
