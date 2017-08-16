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

import org.apache.hive.service.cli.thrift.THandleIdentifier;




public abstract class Handle {

  private final HandleIdentifier handleId;

  public Handle() {
    handleId = new HandleIdentifier();
  }

  public Handle(HandleIdentifier handleId) {
    this.handleId = handleId;
  }

  public Handle(THandleIdentifier tHandleIdentifier) {
    this.handleId = new HandleIdentifier(tHandleIdentifier);
  }

  public HandleIdentifier getHandleIdentifier() {
    return handleId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((handleId == null) ? 0 : handleId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Handle)) {
      return false;
    }
    Handle other = (Handle) obj;
    if (handleId == null) {
      if (other.handleId != null) {
        return false;
      }
    } else if (!handleId.equals(other.handleId)) {
      return false;
    }
    return true;
  }

  @Override
  public abstract String toString();

}
