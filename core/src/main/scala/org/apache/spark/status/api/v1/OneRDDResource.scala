/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import javax.ws.rs.{GET, PathParam, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.ui.SparkUI

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OneRDDResource(ui: SparkUI) {

  @GET
  def rddData(@PathParam("rddId") rddId: Int): RDDStorageInfo = {
    AllRDDResource.getRDDStorageInfo(rddId, ui.storageListener, true).getOrElse(
      throw new NotFoundException(s"no rdd found w/ id $rddId")
    )
  }

}
