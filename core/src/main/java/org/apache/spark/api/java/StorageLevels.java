/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.java;

import org.apache.spark.storage.StorageLevel;

/**
 * Expose some commonly useful storage level constants.
 */
public class StorageLevels {
  public static final StorageLevel NONE = create(false, false, false, false, 1);
  public static final StorageLevel DISK_ONLY = create(true, false, false, false, 1);
  public static final StorageLevel DISK_ONLY_2 = create(true, false, false, false, 2);
  public static final StorageLevel MEMORY_ONLY = create(false, true, false, true, 1);
  public static final StorageLevel MEMORY_ONLY_2 = create(false, true, false, true, 2);
  public static final StorageLevel MEMORY_ONLY_SER = create(false, true, false, false, 1);
  public static final StorageLevel MEMORY_ONLY_SER_2 = create(false, true, false, false, 2);
  public static final StorageLevel MEMORY_AND_DISK = create(true, true, false, true, 1);
  public static final StorageLevel MEMORY_AND_DISK_2 = create(true, true, false, true, 2);
  public static final StorageLevel MEMORY_AND_DISK_SER = create(true, true, false, false, 1);
  public static final StorageLevel MEMORY_AND_DISK_SER_2 = create(true, true, false, false, 2);
  public static final StorageLevel OFF_HEAP = create(true, true, true, false, 1);

  /**
   * Create a new StorageLevel object.
   * @param useDisk saved to disk, if true
   * @param useMemory saved to on-heap memory, if true
   * @param useOffHeap saved to off-heap memory, if true
   * @param deserialized saved as deserialized objects, if true
   * @param replication replication factor
   */
  public static StorageLevel create(
    boolean useDisk,
    boolean useMemory,
    boolean useOffHeap,
    boolean deserialized,
    int replication) {
    return StorageLevel.apply(useDisk, useMemory, useOffHeap, deserialized, replication);
  }
}
