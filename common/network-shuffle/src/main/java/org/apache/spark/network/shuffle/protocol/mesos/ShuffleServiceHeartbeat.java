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

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * A heartbeat sent from the driver to the MesosExternalShuffleService.
 */
public class ShuffleServiceHeartbeat extends BlockTransferMessage {
  private final String appId;

  public ShuffleServiceHeartbeat(String appId) {
    this.appId = appId;
  }

  public String getAppId() { return appId; }

  @Override
  protected Type type() { return Type.HEARTBEAT; }

  @Override
  public int encodedLength() { return Encoders.Strings.encodedLength(appId); }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
  }

  public static ShuffleServiceHeartbeat decode(ByteBuf buf) {
    return new ShuffleServiceHeartbeat(Encoders.Strings.decode(buf));
  }
}
