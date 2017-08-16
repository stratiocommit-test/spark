/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher

/**
 * This class makes SparkSubmitOptionParser visible for Spark code outside of the `launcher`
 * package, since Java doesn't have a feature similar to `private[spark]`, and we don't want
 * that class to be public.
 */
private[spark] abstract class SparkSubmitArgumentsParser extends SparkSubmitOptionParser
