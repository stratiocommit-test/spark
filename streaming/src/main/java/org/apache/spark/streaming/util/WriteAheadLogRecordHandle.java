/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util;

/**
 * :: DeveloperApi ::
 *
 * This abstract class represents a handle that refers to a record written in a
 * {@link org.apache.spark.streaming.util.WriteAheadLog WriteAheadLog}.
 * It must contain all the information necessary for the record to be read and returned by
 * an implementation of the WriteAheadLog class.
 *
 * @see org.apache.spark.streaming.util.WriteAheadLog
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLogRecordHandle implements java.io.Serializable {
}
