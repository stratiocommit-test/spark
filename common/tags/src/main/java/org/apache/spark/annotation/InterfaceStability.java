/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.annotation;

import java.lang.annotation.Documented;

/**
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time.
 */
public class InterfaceStability {

  /**
   * Stable APIs that retain source and binary compatibility within a major release.
   * These interfaces can change from one major release to another major release
   * (e.g. from 1.0 to 2.0).
   */
  @Documented
  public @interface Stable {};

  /**
   * APIs that are meant to evolve towards becoming stable APIs, but are not stable APIs yet.
   * Evolving interfaces can change from one feature release to another release (i.e. 2.1 to 2.2).
   */
  @Documented
  public @interface Evolving {};

  /**
   * Unstable APIs, with no guarantee on stability.
   * Classes that are unannotated are considered Unstable.
   */
  @Documented
  public @interface Unstable {};
}
