/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli.session;

/**
 * Proxy wrapper on HiveSession to execute operations
 * by impersonating given user
 */
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.HiveSQLException;

public class HiveSessionProxy implements InvocationHandler {
  private final HiveSession base;
  private final UserGroupInformation ugi;

  public HiveSessionProxy(HiveSession hiveSession, UserGroupInformation ugi) {
    this.base = hiveSession;
    this.ugi = ugi;
  }

  public static HiveSession getProxy(HiveSession hiveSession, UserGroupInformation ugi)
      throws IllegalArgumentException, HiveSQLException {
    return (HiveSession)Proxy.newProxyInstance(HiveSession.class.getClassLoader(),
        new Class<?>[] {HiveSession.class},
        new HiveSessionProxy(hiveSession, ugi));
  }

  @Override
  public Object invoke(Object arg0, final Method method, final Object[] args)
      throws Throwable {
    try {
      if (method.getDeclaringClass() == HiveSessionBase.class) {
        return invoke(method, args);
      }
      return ugi.doAs(
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws HiveSQLException {
            return invoke(method, args);
          }
        });
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw innerException.getCause();
      } else {
        throw e.getCause();
      }
    }
  }

  private Object invoke(final Method method, final Object[] args) throws HiveSQLException {
    try {
      return method.invoke(base, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HiveSQLException) {
        throw (HiveSQLException)e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}

