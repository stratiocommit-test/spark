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

import java.io.IOException;
import java.util.Map;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;

public final class KerberosSaslHelper {

  public static TProcessorFactory getKerberosProcessorFactory(Server saslServer,
    ThriftCLIService service) {
    return new CLIServiceProcessorFactory(saslServer, service);
  }

  public static TTransport getKerberosTransport(String principal, String host,
    TTransport underlyingTransport, Map<String, String> saslProps, boolean assumeSubject)
    throws SaslException {
    try {
      String[] names = principal.split("[/@]");
      if (names.length != 3) {
        throw new IllegalArgumentException("Kerberos principal should have 3 parts: " + principal);
      }

      if (assumeSubject) {
        return createSubjectAssumedTransport(principal, underlyingTransport, saslProps);
      } else {
        HadoopThriftAuthBridge.Client authBridge =
          ShimLoader.getHadoopThriftAuthBridge().createClientWithConf("kerberos");
        return authBridge.createClientTransport(principal, host, "KERBEROS", null,
                                                underlyingTransport, saslProps);
      }
    } catch (IOException e) {
      throw new SaslException("Failed to open client transport", e);
    }
  }

  public static TTransport createSubjectAssumedTransport(String principal,
    TTransport underlyingTransport, Map<String, String> saslProps) throws IOException {
    String[] names = principal.split("[/@]");
    try {
      TTransport saslTransport =
        new TSaslClientTransport("GSSAPI", null, names[0], names[1], saslProps, null,
          underlyingTransport);
      return new TSubjectAssumingTransport(saslTransport);
    } catch (SaslException se) {
      throw new IOException("Could not instantiate SASL transport", se);
    }
  }

  public static TTransport getTokenTransport(String tokenStr, String host,
    TTransport underlyingTransport, Map<String, String> saslProps) throws SaslException {
    HadoopThriftAuthBridge.Client authBridge =
      ShimLoader.getHadoopThriftAuthBridge().createClientWithConf("kerberos");

    try {
      return authBridge.createClientTransport(null, host, "DIGEST", tokenStr, underlyingTransport,
                                              saslProps);
    } catch (IOException e) {
      throw new SaslException("Failed to open client transport", e);
    }
  }

  private KerberosSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  private static class CLIServiceProcessorFactory extends TProcessorFactory {

    private final ThriftCLIService service;
    private final Server saslServer;

    CLIServiceProcessorFactory(Server saslServer, ThriftCLIService service) {
      super(null);
      this.service = service;
      this.saslServer = saslServer;
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      TProcessor sqlProcessor = new TCLIService.Processor<Iface>(service);
      return saslServer.wrapNonAssumingProcessor(sqlProcessor);
    }
  }
}
