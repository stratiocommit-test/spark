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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;

import org.apache.hadoop.hive.thrift.TFilterTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * This is used on the client side, where the API explicitly opens a transport to
 * the server using the Subject.doAs().
 */
public class TSubjectAssumingTransport extends TFilterTransport {

  public TSubjectAssumingTransport(TTransport wrapped) {
    super(wrapped);
  }

  @Override
  public void open() throws TTransportException {
    try {
      AccessControlContext context = AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {
        public Void run() {
          try {
            wrapped.open();
          } catch (TTransportException tte) {
            // Wrap the transport exception in an RTE, since Subject.doAs() then goes
            // and unwraps this for us out of the doAs block. We then unwrap one
            // more time in our catch clause to get back the TTE. (ugh)
            throw new RuntimeException(tte);
          }
          return null;
        }
      });
    } catch (PrivilegedActionException ioe) {
      throw new RuntimeException("Received an ioe we never threw!", ioe);
    } catch (RuntimeException rte) {
      if (rte.getCause() instanceof TTransportException) {
        throw (TTransportException) rte.getCause();
      } else {
        throw rte;
      }
    }
  }

}
