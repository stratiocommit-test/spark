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

import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.storage.{RDDInfo, StorageStatus, StorageUtils}
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.storage.StorageListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllRDDResource(ui: SparkUI) {

  @GET
  def rddList(): Seq[RDDStorageInfo] = {
    val storageStatusList = ui.storageListener.activeStorageStatusList
    val rddInfos = ui.storageListener.rddInfoList
    rddInfos.map{rddInfo =>
      AllRDDResource.getRDDStorageInfo(rddInfo.id, rddInfo, storageStatusList,
        includeDetails = false)
    }
  }

}

private[spark] object AllRDDResource {

  def getRDDStorageInfo(
      rddId: Int,
      listener: StorageListener,
      includeDetails: Boolean): Option[RDDStorageInfo] = {
    val storageStatusList = listener.activeStorageStatusList
    listener.rddInfoList.find { _.id == rddId }.map { rddInfo =>
      getRDDStorageInfo(rddId, rddInfo, storageStatusList, includeDetails)
    }
  }

  def getRDDStorageInfo(
      rddId: Int,
      rddInfo: RDDInfo,
      storageStatusList: Seq[StorageStatus],
      includeDetails: Boolean): RDDStorageInfo = {
    val workers = storageStatusList.map { (rddId, _) }
    val blockLocations = StorageUtils.getRddBlockLocations(rddId, storageStatusList)
    val blocks = storageStatusList
      .flatMap { _.rddBlocksById(rddId) }
      .sortWith { _._1.name < _._1.name }
      .map { case (blockId, status) =>
        (blockId, status, blockLocations.getOrElse(blockId, Seq[String]("Unknown")))
      }

    val dataDistribution = if (includeDetails) {
      Some(storageStatusList.map { status =>
        new RDDDataDistribution(
          address = status.blockManagerId.hostPort,
          memoryUsed = status.memUsedByRdd(rddId),
          memoryRemaining = status.memRemaining,
          diskUsed = status.diskUsedByRdd(rddId)
        ) } )
    } else {
      None
    }
    val partitions = if (includeDetails) {
      Some(blocks.map { case (id, block, locations) =>
        new RDDPartitionInfo(
          blockName = id.name,
          storageLevel = block.storageLevel.description,
          memoryUsed = block.memSize,
          diskUsed = block.diskSize,
          executors = locations
        )
      } )
    } else {
      None
    }

    new RDDStorageInfo(
      id = rddId,
      name = rddInfo.name,
      numPartitions = rddInfo.numPartitions,
      numCachedPartitions = rddInfo.numCachedPartitions,
      storageLevel = rddInfo.storageLevel.description,
      memoryUsed = rddInfo.memSize,
      diskUsed = rddInfo.diskSize,
      dataDistribution = dataDistribution,
      partitions = partitions
    )
  }
}
