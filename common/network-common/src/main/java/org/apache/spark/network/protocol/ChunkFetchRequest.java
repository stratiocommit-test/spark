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
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
  public final StreamChunkId streamChunkId;

  public ChunkFetchRequest(StreamChunkId streamChunkId) {
    this.streamChunkId = streamChunkId;
  }

  @Override
  public Type type() { return Type.ChunkFetchRequest; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
  }

  public static ChunkFetchRequest decode(ByteBuf buf) {
    return new ChunkFetchRequest(StreamChunkId.decode(buf));
  }

  @Override
  public int hashCode() {
    return streamChunkId.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchRequest) {
      ChunkFetchRequest o = (ChunkFetchRequest) other;
      return streamChunkId.equals(o.streamChunkId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .toString();
  }
}
