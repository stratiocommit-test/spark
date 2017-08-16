/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.annotation.Private;

/**
 * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific
 * comparisons, such as lexicographic comparison for strings.
 */
@Private
public abstract class PrefixComparator {
  public abstract int compare(long prefix1, long prefix2);
}
