/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.server;

import java.nio.ByteBuffer;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/** An RpcHandler suitable for a client-only TransportContext, which cannot receive RPCs. */
public class NoOpRpcHandler extends RpcHandler {
  private final StreamManager streamManager;

  public NoOpRpcHandler() {
    streamManager = new OneForOneStreamManager();
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    throw new UnsupportedOperationException("Cannot handle messages");
  }

  @Override
  public StreamManager getStreamManager() { return streamManager; }
}
