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

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * ::DeveloperApi::
 * TopologyMapper provides topology information for a given host
 * @param conf SparkConf to get required properties, if needed
 */
@DeveloperApi
abstract class TopologyMapper(conf: SparkConf) {
  /**
   * Gets the topology information given the host name
   *
   * @param hostname Hostname
   * @return topology information for the given hostname. One can use a 'topology delimiter'
   *         to make this topology information nested.
   *         For example : ‘/myrack/myhost’, where ‘/’ is the topology delimiter,
   *         ‘myrack’ is the topology identifier, and ‘myhost’ is the individual host.
   *         This function only returns the topology information without the hostname.
   *         This information can be used when choosing executors for block replication
   *         to discern executors from a different rack than a candidate executor, for example.
   *
   *         An implementation can choose to use empty strings or None in case topology info
   *         is not available. This would imply that all such executors belong to the same rack.
   */
  def getTopologyForHost(hostname: String): Option[String]
}

/**
 * A TopologyMapper that assumes all nodes are in the same rack
 */
@DeveloperApi
class DefaultTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  override def getTopologyForHost(hostname: String): Option[String] = {
    logDebug(s"Got a request for $hostname")
    None
  }
}

/**
 * A simple file based topology mapper. This expects topology information provided as a
 * [[java.util.Properties]] file. The name of the file is obtained from SparkConf property
 * `spark.storage.replication.topologyFile`. To use this topology mapper, set the
 * `spark.storage.replication.topologyMapper` property to
 * [[org.apache.spark.storage.FileBasedTopologyMapper]]
 * @param conf SparkConf object
 */
@DeveloperApi
class FileBasedTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  val topologyFile = conf.getOption("spark.storage.replication.topologyFile")
  require(topologyFile.isDefined, "Please specify topology file via " +
    "spark.storage.replication.topologyFile for FileBasedTopologyMapper.")
  val topologyMap = Utils.getPropertiesFromFile(topologyFile.get)

  override def getTopologyForHost(hostname: String): Option[String] = {
    val topology = topologyMap.get(hostname)
    if (topology.isDefined) {
      logDebug(s"$hostname -> ${topology.get}")
    } else {
      logWarning(s"$hostname does not have any topology information")
    }
    topology
  }
}

