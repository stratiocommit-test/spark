/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli;

import java.util.UUID;

import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;


/**
 * SessionHandle.
 *
 */
public class SessionHandle extends Handle {

  private final TProtocolVersion protocol;

  public SessionHandle(TProtocolVersion protocol) {
    this.protocol = protocol;
  }

  // dummy handle for ThriftCLIService
  public SessionHandle(TSessionHandle tSessionHandle) {
    this(tSessionHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
  }

  public SessionHandle(TSessionHandle tSessionHandle, TProtocolVersion protocol) {
    super(tSessionHandle.getSessionId());
    this.protocol = protocol;
  }

  public UUID getSessionId() {
    return getHandleIdentifier().getPublicId();
  }

  public TSessionHandle toTSessionHandle() {
    TSessionHandle tSessionHandle = new TSessionHandle();
    tSessionHandle.setSessionId(getHandleIdentifier().toTHandleIdentifier());
    return tSessionHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return protocol;
  }

  @Override
  public String toString() {
    return "SessionHandle [" + getHandleIdentifier() + "]";
  }
}
