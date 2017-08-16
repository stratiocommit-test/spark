/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.rpc.netty

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import io.netty.channel.Channel
import org.mockito.Matchers._
import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc._

class NettyRpcHandlerSuite extends SparkFunSuite {

  val env = mock(classOf[NettyRpcEnv])
  val sm = mock(classOf[StreamManager])
  when(env.deserialize(any(classOf[TransportClient]), any(classOf[ByteBuffer]))(any()))
    .thenReturn(RequestMessage(RpcAddress("localhost", 12345), null, null))

  test("receive") {
    val dispatcher = mock(classOf[Dispatcher])
    val nettyRpcHandler = new NettyRpcHandler(dispatcher, env, sm)

    val channel = mock(classOf[Channel])
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelActive(client)

    verify(dispatcher, times(1)).postToAll(RemoteProcessConnected(RpcAddress("localhost", 40000)))
  }

  test("connectionTerminated") {
    val dispatcher = mock(classOf[Dispatcher])
    val nettyRpcHandler = new NettyRpcHandler(dispatcher, env, sm)

    val channel = mock(classOf[Channel])
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelActive(client)

    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 40000))
    nettyRpcHandler.channelInactive(client)

    verify(dispatcher, times(1)).postToAll(RemoteProcessConnected(RpcAddress("localhost", 40000)))
    verify(dispatcher, times(1)).postToAll(
      RemoteProcessDisconnected(RpcAddress("localhost", 40000)))
  }

}
