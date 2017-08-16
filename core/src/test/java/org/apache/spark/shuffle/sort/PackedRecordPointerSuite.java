/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.shuffle.sort;

import java.io.IOException;

import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.memory.*;
import org.apache.spark.unsafe.memory.MemoryBlock;

import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;
import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PackedRecordPointerSuite {

  @Test
  public void heap() throws IOException {
    final SparkConf conf = new SparkConf().set("spark.memory.offHeap.enabled", "false");
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryConsumer c = new TestMemoryConsumer(memoryManager, MemoryMode.ON_HEAP);
    final MemoryBlock page0 = memoryManager.allocatePage(128, c);
    final MemoryBlock page1 = memoryManager.allocatePage(128, c);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1,
      page1.getBaseOffset() + 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    assertEquals(360, packedPointer.getPartitionId());
    final long recordPointer = packedPointer.getRecordPointer();
    assertEquals(1, TaskMemoryManager.decodePageNumber(recordPointer));
    assertEquals(page1.getBaseOffset() + 42, memoryManager.getOffsetInPage(recordPointer));
    assertEquals(addressInPage1, recordPointer);
    memoryManager.cleanUpAllAllocatedMemory();
  }

  @Test
  public void offHeap() throws IOException {
    final SparkConf conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10000");
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryConsumer c = new TestMemoryConsumer(memoryManager, MemoryMode.OFF_HEAP);
    final MemoryBlock page0 = memoryManager.allocatePage(128, c);
    final MemoryBlock page1 = memoryManager.allocatePage(128, c);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1,
      page1.getBaseOffset() + 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    assertEquals(360, packedPointer.getPartitionId());
    final long recordPointer = packedPointer.getRecordPointer();
    assertEquals(1, TaskMemoryManager.decodePageNumber(recordPointer));
    assertEquals(page1.getBaseOffset() + 42, memoryManager.getOffsetInPage(recordPointer));
    assertEquals(addressInPage1, recordPointer);
    memoryManager.cleanUpAllAllocatedMemory();
  }

  @Test
  public void maximumPartitionIdCanBeEncoded() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(0, MAXIMUM_PARTITION_ID));
    assertEquals(MAXIMUM_PARTITION_ID, packedPointer.getPartitionId());
  }

  @Test
  public void partitionIdsGreaterThanMaximumPartitionIdWillOverflowOrTriggerError() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    try {
      // Pointers greater than the maximum partition ID will overflow or trigger an assertion error
      packedPointer.set(PackedRecordPointer.packPointer(0, MAXIMUM_PARTITION_ID + 1));
      assertFalse(MAXIMUM_PARTITION_ID  + 1 == packedPointer.getPartitionId());
    } catch (AssertionError e ) {
      // pass
    }
  }

  @Test
  public void maximumOffsetInPageCanBeEncoded() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    long address = TaskMemoryManager.encodePageNumberAndOffset(0, MAXIMUM_PAGE_SIZE_BYTES - 1);
    packedPointer.set(PackedRecordPointer.packPointer(address, 0));
    assertEquals(address, packedPointer.getRecordPointer());
  }

  @Test
  public void offsetsPastMaxOffsetInPageWillOverflow() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    long address = TaskMemoryManager.encodePageNumberAndOffset(0, MAXIMUM_PAGE_SIZE_BYTES);
    packedPointer.set(PackedRecordPointer.packPointer(address, 0));
    assertEquals(0, packedPointer.getRecordPointer());
  }
}
