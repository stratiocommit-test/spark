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

import org.apache.spark.network.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 */
public abstract class MessageHandler<T extends Message> {
  /** Handles the receipt of a single message. */
  public abstract void handle(T message) throws Exception;

  /** Invoked when the channel this MessageHandler is on is active. */
  public abstract void channelActive();

  /** Invoked when an exception was caught on the Channel. */
  public abstract void exceptionCaught(Throwable cause);

  /** Invoked when the channel this MessageHandler is on is inactive. */
  public abstract void channelInactive();
}
