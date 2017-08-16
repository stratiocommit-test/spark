/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Possible values of SASL quality-of-protection value.
 */
public enum SaslQOP {
  // Authentication only.
  AUTH("auth"),
  // Authentication and integrity checking by using signatures.
  AUTH_INT("auth-int"),
  // Authentication, integrity and confidentiality checking by using signatures and encryption.
  AUTH_CONF("auth-conf");

  public final String saslQop;

  private static final Map<String, SaslQOP> STR_TO_ENUM = new HashMap<String, SaslQOP>();

  static {
    for (SaslQOP saslQop : values()) {
      STR_TO_ENUM.put(saslQop.toString(), saslQop);
    }
  }

  SaslQOP(String saslQop) {
    this.saslQop = saslQop;
  }

  public String toString() {
    return saslQop;
  }

  public static SaslQOP fromString(String str) {
    if (str != null) {
      str = str.toLowerCase();
    }
    SaslQOP saslQOP = STR_TO_ENUM.get(str);
    if (saslQOP == null) {
      throw new IllegalArgumentException(
        "Unknown auth type: " + str + " Allowed values are: " + STR_TO_ENUM.keySet());
    }
    return saslQOP;
  }
}
