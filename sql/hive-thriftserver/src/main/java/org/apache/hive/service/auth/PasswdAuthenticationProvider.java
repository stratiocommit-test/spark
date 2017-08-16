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

import javax.security.sasl.AuthenticationException;

public interface PasswdAuthenticationProvider {

  /**
   * The Authenticate method is called by the HiveServer2 authentication layer
   * to authenticate users for their requests.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate {@link AuthenticationException}.
   * <p/>
   * For an example implementation, see {@link LdapAuthenticationProviderImpl}.
   *
   * @param user     The username received over the connection request
   * @param password The password received over the connection request
   *
   * @throws AuthenticationException When a user is found to be
   *                                 invalid by the implementation
   */
  void Authenticate(String user, String password) throws AuthenticationException;
}
