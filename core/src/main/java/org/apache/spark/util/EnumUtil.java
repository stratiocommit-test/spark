/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util;

import com.google.common.base.Joiner;
import org.apache.spark.annotation.Private;

@Private
public class EnumUtil {
  public static <E extends Enum<E>> E parseIgnoreCase(Class<E> clz, String str) {
    E[] constants = clz.getEnumConstants();
    if (str == null) {
      return null;
    }
    for (E e : constants) {
      if (e.name().equalsIgnoreCase(str)) {
        return e;
      }
    }
    throw new IllegalArgumentException(
      String.format("Illegal type='%s'. Supported type values: %s",
        str, Joiner.on(", ").join(constants)));
  }
}
