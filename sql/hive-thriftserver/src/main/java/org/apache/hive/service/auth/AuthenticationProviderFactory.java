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

/**
 * This class helps select a {@link PasswdAuthenticationProvider} for a given {@code AuthMethod}.
 */
public final class AuthenticationProviderFactory {

  public enum AuthMethods {
    LDAP("LDAP"),
    PAM("PAM"),
    CUSTOM("CUSTOM"),
    NONE("NONE");

    private final String authMethod;

    AuthMethods(String authMethod) {
      this.authMethod = authMethod;
    }

    public String getAuthMethod() {
      return authMethod;
    }

    public static AuthMethods getValidAuthMethod(String authMethodStr)
      throws AuthenticationException {
      for (AuthMethods auth : AuthMethods.values()) {
        if (authMethodStr.equals(auth.getAuthMethod())) {
          return auth;
        }
      }
      throw new AuthenticationException("Not a valid authentication method");
    }
  }

  private AuthenticationProviderFactory() {
  }

  public static PasswdAuthenticationProvider getAuthenticationProvider(AuthMethods authMethod)
    throws AuthenticationException {
    if (authMethod == AuthMethods.LDAP) {
      return new LdapAuthenticationProviderImpl();
    } else if (authMethod == AuthMethods.PAM) {
      return new PamAuthenticationProviderImpl();
    } else if (authMethod == AuthMethods.CUSTOM) {
      return new CustomAuthenticationProviderImpl();
    } else if (authMethod == AuthMethods.NONE) {
      return new AnonymousAuthenticationProviderImpl();
    } else {
      throw new AuthenticationException("Unsupported authentication method");
    }
  }
}
