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

import java.sql.{Driver, DriverManager}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * java.sql.DriverManager is always loaded by bootstrap classloader,
 * so it can't load JDBC drivers accessible by Spark ClassLoader.
 *
 * To solve the problem, drivers from user-supplied jars are wrapped into thin wrapper.
 */
object DriverRegistry extends Logging {

  private val wrapperMap: mutable.Map[String, DriverWrapper] = mutable.Map.empty

  def register(className: String): Unit = {
    val cls = Utils.getContextOrSparkClassLoader.loadClass(className)
    if (cls.getClassLoader == null) {
      logTrace(s"$className has been loaded with bootstrap ClassLoader, wrapper is not required")
    } else if (wrapperMap.get(className).isDefined) {
      logTrace(s"Wrapper for $className already exists")
    } else {
      synchronized {
        if (wrapperMap.get(className).isEmpty) {
          val wrapper = new DriverWrapper(cls.newInstance().asInstanceOf[Driver])
          DriverManager.registerDriver(wrapper)
          wrapperMap(className) = wrapper
          logTrace(s"Wrapper for $className registered")
        }
      }
    }
  }
}

