/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.shuffle.protocol.mesos;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * A message sent from the driver to register with the MesosExternalShuffleService.
 */
public class RegisterDriver extends BlockTransferMessage {
  private final String appId;
  private final long heartbeatTimeoutMs;

  public RegisterDriver(String appId, long heartbeatTimeoutMs) {
    this.appId = appId;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
  }

  public String getAppId() { return appId; }

  public long getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }

  @Override
  protected Type type() { return Type.REGISTER_DRIVER; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId) + Long.SIZE / Byte.SIZE;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeLong(heartbeatTimeoutMs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, heartbeatTimeoutMs);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RegisterDriver)) {
      return false;
    }
    return Objects.equal(appId, ((RegisterDriver) o).appId);
  }

  public static RegisterDriver decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    long heartbeatTimeout = buf.readLong();
    return new RegisterDriver(appId, heartbeatTimeout);
  }
}
