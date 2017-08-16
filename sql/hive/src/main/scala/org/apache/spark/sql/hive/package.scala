/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

/**
 * Support for running Spark SQL queries using functionality from Apache Hive (does not require an
 * existing Hive installation).  Supported Hive features include:
 *  - Using HiveQL to express queries.
 *  - Reading metadata from the Hive Metastore using HiveSerDes.
 *  - Hive UDFs, UDAs, UDTs
 *
 * Users that would like access to this functionality should create a
 * [[hive.HiveContext HiveContext]] instead of a [[SQLContext]].
 */
package object hive
