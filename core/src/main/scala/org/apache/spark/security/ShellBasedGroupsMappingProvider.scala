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

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This class is responsible for getting the groups for a particular user in Unix based
 * environments. This implementation uses the Unix Shell based id command to fetch the user groups
 * for the specified user. It does not cache the user groups as the invocations are expected
 * to be infrequent.
 */

private[spark] class ShellBasedGroupsMappingProvider extends GroupMappingServiceProvider
  with Logging {

  override def getGroups(username: String): Set[String] = {
    val userGroups = getUnixGroups(username)
    logDebug("User: " + username + " Groups: " + userGroups.mkString(","))
    userGroups
  }

  // shells out a "bash -c id -Gn username" to get user groups
  private def getUnixGroups(username: String): Set[String] = {
    val cmdSeq = Seq("bash", "-c", "id -Gn " + username)
    // we need to get rid of the trailing "\n" from the result of command execution
    Utils.executeAndGetOutput(cmdSeq).stripLineEnd.split(" ").toSet
  }
}
