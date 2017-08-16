/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * Initial registration message between an executor and its local shuffle server.
 * Returns nothing (empty byte array).
 */
public class RegisterExecutor extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final ExecutorShuffleInfo executorInfo;

  public RegisterExecutor(
      String appId,
      String execId,
      ExecutorShuffleInfo executorInfo) {
    this.appId = appId;
    this.execId = execId;
    this.executorInfo = executorInfo;
  }

  @Override
  protected Type type() { return Type.REGISTER_EXECUTOR; }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, execId, executorInfo);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("executorInfo", executorInfo)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof RegisterExecutor) {
      RegisterExecutor o = (RegisterExecutor) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(execId, o.execId)
        && Objects.equal(executorInfo, o.executorInfo);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + executorInfo.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    executorInfo.encode(buf);
  }

  public static RegisterExecutor decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
    return new RegisterExecutor(appId, execId, executorShuffleInfo);
  }
}
