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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.JavaUtils;

/**
 * A class that manages shuffle secret used by the external shuffle service.
 */
public class ShuffleSecretManager implements SecretKeyHolder {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleSecretManager.class);

  private final ConcurrentHashMap<String, String> shuffleSecretMap;

  // Spark user used for authenticating SASL connections
  // Note that this must match the value in org.apache.spark.SecurityManager
  private static final String SPARK_SASL_USER = "sparkSaslUser";

  public ShuffleSecretManager() {
    shuffleSecretMap = new ConcurrentHashMap<>();
  }

  /**
   * Register an application with its secret.
   * Executors need to first authenticate themselves with the same secret before
   * fetching shuffle files written by other executors in this application.
   */
  public void registerApp(String appId, String shuffleSecret) {
    if (!shuffleSecretMap.contains(appId)) {
      shuffleSecretMap.put(appId, shuffleSecret);
      logger.info("Registered shuffle secret for application {}", appId);
    } else {
      logger.debug("Application {} already registered", appId);
    }
  }

  /**
   * Register an application with its secret specified as a byte buffer.
   */
  public void registerApp(String appId, ByteBuffer shuffleSecret) {
    registerApp(appId, JavaUtils.bytesToString(shuffleSecret));
  }

  /**
   * Unregister an application along with its secret.
   * This is called when the application terminates.
   */
  public void unregisterApp(String appId) {
    if (shuffleSecretMap.contains(appId)) {
      shuffleSecretMap.remove(appId);
      logger.info("Unregistered shuffle secret for application {}", appId);
    } else {
      logger.warn("Attempted to unregister application {} when it is not registered", appId);
    }
  }

  /**
   * Return the Spark user for authenticating SASL connections.
   */
  @Override
  public String getSaslUser(String appId) {
    return SPARK_SASL_USER;
  }

  /**
   * Return the secret key registered with the given application.
   * This key is used to authenticate the executors before they can fetch shuffle files
   * written by this application from the external shuffle service. If the specified
   * application is not registered, return null.
   */
  @Override
  public String getSecretKey(String appId) {
    return shuffleSecretMap.get(appId);
  }
}
