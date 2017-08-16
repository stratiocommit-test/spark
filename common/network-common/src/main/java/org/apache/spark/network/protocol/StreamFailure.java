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
 * Message indicating an error when transferring a stream.
 */
public final class StreamFailure extends AbstractMessage implements ResponseMessage {
  public final String streamId;
  public final String error;

  public StreamFailure(String streamId, String error) {
    this.streamId = streamId;
    this.error = error;
  }

  @Override
  public Type type() { return Type.StreamFailure; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(streamId) + Encoders.Strings.encodedLength(error);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, streamId);
    Encoders.Strings.encode(buf, error);
  }

  public static StreamFailure decode(ByteBuf buf) {
    String streamId = Encoders.Strings.decode(buf);
    String error = Encoders.Strings.decode(buf);
    return new StreamFailure(streamId, error);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, error);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamFailure) {
      StreamFailure o = (StreamFailure) other;
      return streamId.equals(o.streamId) && error.equals(o.error);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("error", error)
      .toString();
  }

}
