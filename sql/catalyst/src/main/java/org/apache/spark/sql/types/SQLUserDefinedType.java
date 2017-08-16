/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.types;

import java.lang.annotation.*;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.annotation.InterfaceStability;

/**
 * ::DeveloperApi::
 * A user-defined type which can be automatically recognized by a SQLContext and registered.
 * WARNING: UDTs are currently only supported from Scala.
 */
// TODO: Should I used @Documented ?
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@InterfaceStability.Evolving
public @interface SQLUserDefinedType {

  /**
   * Returns an instance of the UserDefinedType which can serialize and deserialize the user
   * class to and from Catalyst built-in types.
   */
  Class<? extends UserDefinedType<?>> udt();
}
