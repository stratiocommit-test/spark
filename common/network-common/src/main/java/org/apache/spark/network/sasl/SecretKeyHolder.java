/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.sasl;

/**
 * Interface for getting a secret key associated with some application.
 */
public interface SecretKeyHolder {
  /**
   * Gets an appropriate SASL User for the given appId.
   * @throws IllegalArgumentException if the given appId is not associated with a SASL user.
   */
  String getSaslUser(String appId);

  /**
   * Gets an appropriate SASL secret key for the given appId.
   * @throws IllegalArgumentException if the given appId is not associated with a SASL secret key.
   */
  String getSecretKey(String appId);
}
