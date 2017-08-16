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
 * Response to {@link ChunkFetchRequest} when there is an error fetching the chunk.
 */
public final class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {
  public final StreamChunkId streamChunkId;
  public final String errorString;

  public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString) {
    this.streamChunkId = streamChunkId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.ChunkFetchFailure; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + Encoders.Strings.encodedLength(errorString);
  }

  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    Encoders.Strings.encode(buf, errorString);
  }

  public static ChunkFetchFailure decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    String errorString = Encoders.Strings.decode(buf);
    return new ChunkFetchFailure(streamChunkId, errorString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, errorString);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchFailure) {
      ChunkFetchFailure o = (ChunkFetchFailure) other;
      return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("errorString", errorString)
      .toString();
  }
}
