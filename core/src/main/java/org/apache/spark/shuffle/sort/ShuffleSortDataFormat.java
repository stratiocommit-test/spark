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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.SortDataFormat;

final class ShuffleSortDataFormat extends SortDataFormat<PackedRecordPointer, LongArray> {

  private final LongArray buffer;

  ShuffleSortDataFormat(LongArray buffer) {
    this.buffer = buffer;
  }

  @Override
  public PackedRecordPointer getKey(LongArray data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public PackedRecordPointer newKey() {
    return new PackedRecordPointer();
  }

  @Override
  public PackedRecordPointer getKey(LongArray data, int pos, PackedRecordPointer reuse) {
    reuse.set(data.get(pos));
    return reuse;
  }

  @Override
  public void swap(LongArray data, int pos0, int pos1) {
    final long temp = data.get(pos0);
    data.set(pos0, data.get(pos1));
    data.set(pos1, temp);
  }

  @Override
  public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
    dst.set(dstPos, src.get(srcPos));
  }

  @Override
  public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
    Platform.copyMemory(
      src.getBaseObject(),
      src.getBaseOffset() + srcPos * 8L,
      dst.getBaseObject(),
      dst.getBaseOffset() + dstPos * 8L,
      length * 8L
    );
  }

  @Override
  public LongArray allocate(int length) {
    assert (length <= buffer.size()) :
      "the buffer is smaller than required: " + buffer.size() + " < " + length;
    return buffer;
  }
}
