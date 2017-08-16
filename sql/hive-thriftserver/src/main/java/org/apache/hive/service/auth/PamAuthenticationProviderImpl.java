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

import net.sf.jpam.Pam;
import org.apache.hadoop.hive.conf.HiveConf;

public class PamAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private final String pamServiceNames;

  PamAuthenticationProviderImpl() {
    HiveConf conf = new HiveConf();
    pamServiceNames = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES);
  }

  @Override
  public void Authenticate(String user, String password) throws AuthenticationException {

    if (pamServiceNames == null || pamServiceNames.trim().isEmpty()) {
      throw new AuthenticationException("No PAM services are set.");
    }

    String[] pamServices = pamServiceNames.split(",");
    for (String pamService : pamServices) {
      Pam pam = new Pam(pamService);
      boolean isAuthenticated = pam.authenticateSuccessful(user, password);
      if (!isAuthenticated) {
        throw new AuthenticationException(
          "Error authenticating with the PAM service: " + pamService);
      }
    }
  }
}
