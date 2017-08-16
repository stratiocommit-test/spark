/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
* Encapsulates a request for a particular chunk of a stream.
*/
public final class StreamChunkId implements Encodable {
  public final long streamId;
  public final int chunkIndex;

  public StreamChunkId(long streamId, int chunkIndex) {
    this.streamId = streamId;
    this.chunkIndex = chunkIndex;
  }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  public void encode(ByteBuf buffer) {
    buffer.writeLong(streamId);
    buffer.writeInt(chunkIndex);
  }

  public static StreamChunkId decode(ByteBuf buffer) {
    assert buffer.readableBytes() >= 8 + 4;
    long streamId = buffer.readLong();
    int chunkIndex = buffer.readInt();
    return new StreamChunkId(streamId, chunkIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, chunkIndex);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamChunkId) {
      StreamChunkId o = (StreamChunkId) other;
      return streamId == o.streamId && chunkIndex == o.chunkIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("chunkIndex", chunkIndex)
      .toString();
  }
}
