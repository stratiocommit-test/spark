/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 *
 * Represents the state of a StreamingContext.
 */
@DeveloperApi
public enum StreamingContextState {
  /**
   * The context has been created, but not been started yet.
   * Input DStreams, transformations and output operations can be created on the context.
   */
  INITIALIZED,

  /**
   * The context has been started, and been not stopped.
   * Input DStreams, transformations and output operations cannot be created on the context.
   */
  ACTIVE,

  /**
   * The context has been stopped and cannot be used any more.
   */
  STOPPED
}
