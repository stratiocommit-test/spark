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

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hive.service.cli.thrift.THandleIdentifier;

/**
 * HandleIdentifier.
 *
 */
public class HandleIdentifier {
  private final UUID publicId;
  private final UUID secretId;

  public HandleIdentifier() {
    publicId = UUID.randomUUID();
    secretId = UUID.randomUUID();
  }

  public HandleIdentifier(UUID publicId, UUID secretId) {
    this.publicId = publicId;
    this.secretId = secretId;
  }

  public HandleIdentifier(THandleIdentifier tHandleId) {
    ByteBuffer bb = ByteBuffer.wrap(tHandleId.getGuid());
    this.publicId = new UUID(bb.getLong(), bb.getLong());
    bb = ByteBuffer.wrap(tHandleId.getSecret());
    this.secretId = new UUID(bb.getLong(), bb.getLong());
  }

  public UUID getPublicId() {
    return publicId;
  }

  public UUID getSecretId() {
    return secretId;
  }

  public THandleIdentifier toTHandleIdentifier() {
    byte[] guid = new byte[16];
    byte[] secret = new byte[16];
    ByteBuffer guidBB = ByteBuffer.wrap(guid);
    ByteBuffer secretBB = ByteBuffer.wrap(secret);
    guidBB.putLong(publicId.getMostSignificantBits());
    guidBB.putLong(publicId.getLeastSignificantBits());
    secretBB.putLong(secretId.getMostSignificantBits());
    secretBB.putLong(secretId.getLeastSignificantBits());
    return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((publicId == null) ? 0 : publicId.hashCode());
    result = prime * result + ((secretId == null) ? 0 : secretId.hashCode());
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
    if (!(obj instanceof HandleIdentifier)) {
      return false;
    }
    HandleIdentifier other = (HandleIdentifier) obj;
    if (publicId == null) {
      if (other.publicId != null) {
        return false;
      }
    } else if (!publicId.equals(other.publicId)) {
      return false;
    }
    if (secretId == null) {
      if (other.secretId != null) {
        return false;
      }
    } else if (!secretId.equals(other.secretId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return publicId.toString();
  }
}
