/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverPropertyInfo, SQLFeatureNotSupportedException}
import java.util.Properties

/**
 * A wrapper for a JDBC Driver to work around SPARK-6913.
 *
 * The problem is in `java.sql.DriverManager` class that can't access drivers loaded by
 * Spark ClassLoader.
 */
class DriverWrapper(val wrapped: Driver) extends Driver {
  override def acceptsURL(url: String): Boolean = wrapped.acceptsURL(url)

  override def jdbcCompliant(): Boolean = wrapped.jdbcCompliant()

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    wrapped.getPropertyInfo(url, info)
  }

  override def getMinorVersion: Int = wrapped.getMinorVersion

  def getParentLogger: java.util.logging.Logger = {
    throw new SQLFeatureNotSupportedException(
      s"${this.getClass.getName}.getParentLogger is not yet implemented.")
  }

  override def connect(url: String, info: Properties): Connection = wrapped.connect(url, info)

  override def getMajorVersion: Int = wrapped.getMajorVersion
}
