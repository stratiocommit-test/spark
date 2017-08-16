/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

import java.util.HashSet;
import java.util.Set;

public enum TaskSorting {
  ID,
  INCREASING_RUNTIME("runtime"),
  DECREASING_RUNTIME("-runtime");

  private final Set<String> alternateNames;
  TaskSorting(String... names) {
    alternateNames = new HashSet<>();
    for (String n: names) {
      alternateNames.add(n);
    }
  }

  public static TaskSorting fromString(String str) {
    String lower = str.toLowerCase();
    for (TaskSorting t: values()) {
      if (t.alternateNames.contains(lower)) {
        return t;
      }
    }
    return EnumUtil.parseIgnoreCase(TaskSorting.class, str);
  }

}
