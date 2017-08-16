/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.Serializable;
import java.util.logging.Handler;
import java.util.logging.Logger;

import org.apache.parquet.Log;
import org.slf4j.bridge.SLF4JBridgeHandler;

// Redirects the JUL logging for parquet-mr versions <= 1.8 to SLF4J logging using
// SLF4JBridgeHandler. Parquet-mr versions >= 1.9 use SLF4J directly
final class ParquetLogRedirector implements Serializable {
  // Client classes should hold a reference to INSTANCE to ensure redirection occurs. This is
  // especially important for Serializable classes where fields are set but constructors are
  // ignored
  static final ParquetLogRedirector INSTANCE = new ParquetLogRedirector();

  // JUL loggers must be held by a strong reference, otherwise they may get destroyed by GC.
  // However, the root JUL logger used by Parquet isn't properly referenced.  Here we keep
  // references to loggers in both parquet-mr <= 1.6 and 1.7/1.8
  private static final Logger apacheParquetLogger =
    Logger.getLogger(Log.class.getPackage().getName());
  private static final Logger parquetLogger = Logger.getLogger("parquet");

  static {
    // For parquet-mr 1.7 and 1.8, which are under `org.apache.parquet` namespace.
    try {
      Class.forName(Log.class.getName());
      redirect(Logger.getLogger(Log.class.getPackage().getName()));
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    // For parquet-mr 1.6.0 and lower versions bundled with Hive, which are under `parquet`
    // namespace.
    try {
      Class.forName("parquet.Log");
      redirect(Logger.getLogger("parquet"));
    } catch (Throwable t) {
      // SPARK-9974: com.twitter:parquet-hadoop-bundle:1.6.0 is not packaged into the assembly
      // when Spark is built with SBT. So `parquet.Log` may not be found.  This try/catch block
      // should be removed after this issue is fixed.
    }
  }

  private ParquetLogRedirector() {
  }

  private static void redirect(Logger logger) {
    for (Handler handler : logger.getHandlers()) {
      logger.removeHandler(handler);
    }
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JBridgeHandler());
  }
}
