/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.spark.launcher.LauncherProtocol.*;

public class LauncherServerSuite extends BaseSuite {

  @Test
  public void testLauncherServerReuse() throws Exception {
    ChildProcAppHandle handle1 = null;
    ChildProcAppHandle handle2 = null;
    ChildProcAppHandle handle3 = null;

    try {
      handle1 = LauncherServer.newAppHandle();
      handle2 = LauncherServer.newAppHandle();
      LauncherServer server1 = handle1.getServer();
      assertSame(server1, handle2.getServer());

      handle1.kill();
      handle2.kill();

      handle3 = LauncherServer.newAppHandle();
      assertNotSame(server1, handle3.getServer());

      handle3.kill();

      assertNull(LauncherServer.getServerInstance());
    } finally {
      kill(handle1);
      kill(handle2);
      kill(handle3);
    }
  }

  @Test
  public void testCommunication() throws Exception {
    ChildProcAppHandle handle = LauncherServer.newAppHandle();
    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());

      final Semaphore semaphore = new Semaphore(0);
      handle.addListener(new SparkAppHandle.Listener() {
        @Override
        public void stateChanged(SparkAppHandle handle) {
          semaphore.release();
        }
        @Override
        public void infoChanged(SparkAppHandle handle) {
          semaphore.release();
        }
      });

      client = new TestClient(s);
      client.send(new Hello(handle.getSecret(), "1.4.0"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));

      // Make sure the server matched the client to the handle.
      assertNotNull(handle.getConnection());

      client.send(new SetAppId("app-id"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      assertEquals("app-id", handle.getAppId());

      client.send(new SetState(SparkAppHandle.State.RUNNING));
      assertTrue(semaphore.tryAcquire(1, TimeUnit.SECONDS));
      assertEquals(SparkAppHandle.State.RUNNING, handle.getState());

      handle.stop();
      Message stopMsg = client.inbound.poll(30, TimeUnit.SECONDS);
      assertTrue(stopMsg instanceof Stop);
    } finally {
      kill(handle);
      close(client);
      client.clientThread.join();
    }
  }

  @Test
  public void testTimeout() throws Exception {
    ChildProcAppHandle handle = null;
    TestClient client = null;
    try {
      // LauncherServer will immediately close the server-side socket when the timeout is set
      // to 0.
      SparkLauncher.setConfig(SparkLauncher.CHILD_CONNECTION_TIMEOUT, "0");

      handle = LauncherServer.newAppHandle();

      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());
      client = new TestClient(s);

      // Try a few times since the client-side socket may not reflect the server-side close
      // immediately.
      boolean helloSent = false;
      int maxTries = 10;
      for (int i = 0; i < maxTries; i++) {
        try {
          if (!helloSent) {
            client.send(new Hello(handle.getSecret(), "1.4.0"));
            helloSent = true;
          } else {
            client.send(new SetAppId("appId"));
          }
          fail("Expected exception caused by connection timeout.");
        } catch (IllegalStateException | IOException e) {
          // Expected.
          break;
        } catch (AssertionError e) {
          if (i < maxTries - 1) {
            Thread.sleep(100);
          } else {
            throw new AssertionError("Test failed after " + maxTries + " attempts.", e);
          }
        }
      }
    } finally {
      SparkLauncher.launcherConfig.remove(SparkLauncher.CHILD_CONNECTION_TIMEOUT);
      kill(handle);
      close(client);
    }
  }

  @Test
  public void testSparkSubmitVmShutsDown() throws Exception {
    ChildProcAppHandle handle = LauncherServer.newAppHandle();
    TestClient client = null;
    final Semaphore semaphore = new Semaphore(0);
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());
      handle.addListener(new SparkAppHandle.Listener() {
        public void stateChanged(SparkAppHandle handle) {
          semaphore.release();
        }
        public void infoChanged(SparkAppHandle handle) {
          semaphore.release();
        }
      });
      client = new TestClient(s);
      client.send(new Hello(handle.getSecret(), "1.4.0"));
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      // Make sure the server matched the client to the handle.
      assertNotNull(handle.getConnection());
      close(client);
      assertTrue(semaphore.tryAcquire(30, TimeUnit.SECONDS));
      assertEquals(SparkAppHandle.State.LOST, handle.getState());
    } finally {
      kill(handle);
      close(client);
      client.clientThread.join();
    }
  }

  private void kill(SparkAppHandle handle) {
    if (handle != null) {
      handle.kill();
    }
  }

  private void close(Closeable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception e) {
        // no-op.
      }
    }
  }

  private static class TestClient extends LauncherConnection {

    final BlockingQueue<Message> inbound;
    final Thread clientThread;

    TestClient(Socket s) throws IOException {
      super(s);
      this.inbound = new LinkedBlockingQueue<>();
      this.clientThread = new Thread(this);
      clientThread.setName("TestClient");
      clientThread.setDaemon(true);
      clientThread.start();
    }

    @Override
    protected void handle(Message msg) throws IOException {
      inbound.offer(msg);
    }

  }

}
