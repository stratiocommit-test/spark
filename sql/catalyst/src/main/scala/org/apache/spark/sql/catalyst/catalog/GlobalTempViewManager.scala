/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.catalog

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils


/**
 * A thread-safe manager for global temporary views, providing atomic operations to manage them,
 * e.g. create, update, remove, etc.
 *
 * Note that, the view name is always case-sensitive here, callers are responsible to format the
 * view name w.r.t. case-sensitive config.
 *
 * @param database The system preserved virtual database that keeps all the global temporary views.
 */
class GlobalTempViewManager(val database: String) {

  /** List of view definitions, mapping from view name to logical plan. */
  @GuardedBy("this")
  private val viewDefinitions = new mutable.HashMap[String, LogicalPlan]

  /**
   * Returns the global view definition which matches the given name, or None if not found.
   */
  def get(name: String): Option[LogicalPlan] = synchronized {
    viewDefinitions.get(name)
  }

  /**
   * Creates a global temp view, or issue an exception if the view already exists and
   * `overrideIfExists` is false.
   */
  def create(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    if (!overrideIfExists && viewDefinitions.contains(name)) {
      throw new TempTableAlreadyExistsException(name)
    }
    viewDefinitions.put(name, viewDefinition)
  }

  /**
   * Updates the global temp view if it exists, returns true if updated, false otherwise.
   */
  def update(
      name: String,
      viewDefinition: LogicalPlan): Boolean = synchronized {
    if (viewDefinitions.contains(name)) {
      viewDefinitions.put(name, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   * Removes the global temp view if it exists, returns true if removed, false otherwise.
   */
  def remove(name: String): Boolean = synchronized {
    viewDefinitions.remove(name).isDefined
  }

  /**
   * Renames the global temp view if the source view exists and the destination view not exists, or
   * issue an exception if the source view exists but the destination view already exists. Returns
   * true if renamed, false otherwise.
   */
  def rename(oldName: String, newName: String): Boolean = synchronized {
    if (viewDefinitions.contains(oldName)) {
      if (viewDefinitions.contains(newName)) {
        throw new AnalysisException(
          s"rename temporary view from '$oldName' to '$newName': destination view already exists")
      }

      val viewDefinition = viewDefinitions(oldName)
      viewDefinitions.remove(oldName)
      viewDefinitions.put(newName, viewDefinition)
      true
    } else {
      false
    }
  }

  /**
   * Lists the names of all global temporary views.
   */
  def listViewNames(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(viewDefinitions.keys.toSeq, pattern)
  }

  /**
   * Clears all the global temporary views.
   */
  def clear(): Unit = synchronized {
    viewDefinitions.clear()
  }
}
