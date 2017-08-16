/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
/**
 * RDD-based machine learning APIs (in maintenance mode).
 *
 * The <code>spark.mllib</code> package is in maintenance mode as of the Spark 2.0.0 release to
 * encourage migration to the DataFrame-based APIs under the <code>spark.ml</code> package.
 * While in maintenance mode,
 * <ul>
 *   <li>
 *     no new features in the RDD-based <code>spark.mllib</code> package will be accepted, unless
 *     they block implementing new features in the DataFrame-based <code>spark.ml</code> package;
 *   </li>
 *   <li>
 *     bug fixes in the RDD-based APIs will still be accepted.
 *   </li>
 * </ul>
 *
 * The developers will continue adding more features to the DataFrame-based APIs in the 2.x series
 * to reach feature parity with the RDD-based APIs.
 * And once we reach feature parity, this package will be deprecated.
 *
 * @see <a href="https://issues.apache.org/jira/browse/SPARK-4591" target="_blank">SPARK-4591</a> to
 *      track the progress of feature parity
 */
package org.apache.spark.mllib;
