/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.protocol.AbstractMessage;

/**
 * Encodes a Sasl-related message which is attempting to authenticate using some credentials tagged
 * with the given appId. This appId allows a single SaslRpcHandler to multiplex different
 * applications which may be using different sets of credentials.
 */
class SaslMessage extends AbstractMessage {

  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEA;

  public final String appId;

  SaslMessage(String appId, byte[] message) {
    this(appId, Unpooled.wrappedBuffer(message));
  }

  SaslMessage(String appId, ByteBuf message) {
    super(new NettyManagedBuffer(message), true);
    this.appId = appId;
  }

  @Override
  public Type type() { return Type.User; }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 1 + Encoders.Strings.encodedLength(appId) + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    Encoders.Strings.encode(buf, appId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  public static SaslMessage decode(ByteBuf buf) {
    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else"
        + " (maybe your client does not have SASL enabled?)");
    }

    String appId = Encoders.Strings.decode(buf);
    // See comment in encodedLength().
    buf.readInt();
    return new SaslMessage(appId, buf.retain());
  }
}
