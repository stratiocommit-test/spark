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
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.launcher.LauncherProtocol.*;

/**
 * Encapsulates a connection between a launcher server and client. This takes care of the
 * communication (sending and receiving messages), while processing of messages is left for
 * the implementations.
 */
abstract class LauncherConnection implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(LauncherConnection.class.getName());

  private final Socket socket;
  private final ObjectOutputStream out;

  private volatile boolean closed;

  LauncherConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.out = new ObjectOutputStream(socket.getOutputStream());
    this.closed = false;
  }

  protected abstract void handle(Message msg) throws IOException;

  @Override
  public void run() {
    try {
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      while (!closed) {
        Message msg = (Message) in.readObject();
        handle(msg);
      }
    } catch (EOFException eof) {
      // Remote side has closed the connection, just cleanup.
      try {
        close();
      } catch (Exception unused) {
        // no-op.
      }
    } catch (Exception e) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error in inbound message handling.", e);
        try {
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
    }
  }

  protected synchronized void send(Message msg) throws IOException {
    try {
      CommandBuilderUtils.checkState(!closed, "Disconnected.");
      out.writeObject(msg);
      out.flush();
    } catch (IOException ioe) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error when sending message.", ioe);
        try {
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
      throw ioe;
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      synchronized (this) {
        if (!closed) {
          closed = true;
          socket.close();
        }
      }
    }
  }

}
