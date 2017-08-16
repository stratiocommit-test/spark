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
 * Request to stream data from the remote end.
 * <p>
 * The stream ID is an arbitrary string that needs to be negotiated between the two endpoints before
 * the data can be streamed.
 */
public final class StreamRequest extends AbstractMessage implements RequestMessage {
   public final String streamId;

   public StreamRequest(String streamId) {
     this.streamId = streamId;
   }

  @Override
  public Type type() { return Type.StreamRequest; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(streamId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, streamId);
  }

  public static StreamRequest decode(ByteBuf buf) {
    String streamId = Encoders.Strings.decode(buf);
    return new StreamRequest(streamId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamRequest) {
      StreamRequest o = (StreamRequest) other;
      return streamId.equals(o.streamId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .toString();
  }

}
