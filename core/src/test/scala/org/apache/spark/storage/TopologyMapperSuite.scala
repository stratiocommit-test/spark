/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.storage

import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark._
import org.apache.spark.util.Utils

class TopologyMapperSuite  extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  test("File based Topology Mapper") {
    val numHosts = 100
    val numRacks = 4
    val props = (1 to numHosts).map{i => s"host-$i" -> s"rack-${i % numRacks}"}.toMap
    val propsFile = createPropertiesFile(props)

    val sparkConf = (new SparkConf(false))
    sparkConf.set("spark.storage.replication.topologyFile", propsFile.getAbsolutePath)
    val topologyMapper = new FileBasedTopologyMapper(sparkConf)

    props.foreach {case (host, topology) =>
      val obtainedTopology = topologyMapper.getTopologyForHost(host)
      assert(obtainedTopology.isDefined)
      assert(obtainedTopology.get === topology)
    }

    // we get None for hosts not in the file
    assert(topologyMapper.getTopologyForHost("host").isEmpty)

    cleanup(propsFile)
  }

  def createPropertiesFile(props: Map[String, String]): File = {
    val testFile = new File(Utils.createTempDir(), "TopologyMapperSuite-test").getAbsoluteFile
    val fileOS = new FileOutputStream(testFile)
    props.foreach{case (k, v) => fileOS.write(s"$k=$v\n".getBytes)}
    fileOS.close
    testFile
  }

  def cleanup(testFile: File): Unit = {
    testFile.getParentFile.listFiles.filter { file =>
      file.getName.startsWith(testFile.getName)
    }.foreach { _.delete() }
  }

}
