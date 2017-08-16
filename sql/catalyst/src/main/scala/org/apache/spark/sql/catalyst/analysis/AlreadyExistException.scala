/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec

/**
 * Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class DatabaseAlreadyExistsException(db: String)
  extends AnalysisException(s"Database '$db' already exists")

class TableAlreadyExistsException(db: String, table: String)
  extends AnalysisException(s"Table or view '$table' already exists in database '$db'")

class TempTableAlreadyExistsException(table: String)
  extends AnalysisException(s"Temporary table '$table' already exists")

class PartitionAlreadyExistsException(db: String, table: String, spec: TablePartitionSpec)
  extends AnalysisException(
    s"Partition already exists in table '$table' database '$db':\n" + spec.mkString("\n"))

class PartitionsAlreadyExistException(db: String, table: String, specs: Seq[TablePartitionSpec])
  extends AnalysisException(
    s"The following partitions already exists in table '$table' database '$db':\n"
      + specs.mkString("\n===\n"))

class FunctionAlreadyExistsException(db: String, func: String)
  extends AnalysisException(s"Function '$func' already exists in database '$db'")

class TempFunctionAlreadyExistsException(func: String)
  extends AnalysisException(s"Temporary function '$func' already exists")
