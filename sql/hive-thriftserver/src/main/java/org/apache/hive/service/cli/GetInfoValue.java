/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hive.service.cli;

import org.apache.hive.service.cli.thrift.TGetInfoValue;

/**
 * GetInfoValue.
 *
 */
public class GetInfoValue {
  private String stringValue = null;
  private short shortValue;
  private int intValue;
  private long longValue;

  public GetInfoValue(String stringValue) {
    this.stringValue = stringValue;
  }

  public GetInfoValue(short shortValue) {
    this.shortValue = shortValue;
  }

  public GetInfoValue(int intValue) {
    this.intValue = intValue;
  }

  public GetInfoValue(long longValue) {
    this.longValue = longValue;
  }

  public GetInfoValue(TGetInfoValue tGetInfoValue) {
    switch (tGetInfoValue.getSetField()) {
    case STRING_VALUE:
      stringValue = tGetInfoValue.getStringValue();
      break;
    default:
      throw new IllegalArgumentException("Unreconigzed TGetInfoValue");
    }
  }

  public TGetInfoValue toTGetInfoValue() {
    TGetInfoValue tInfoValue = new TGetInfoValue();
    if (stringValue != null) {
      tInfoValue.setStringValue(stringValue);
    }
    return tInfoValue;
  }

  public String getStringValue() {
    return stringValue;
  }

  public short getShortValue() {
    return shortValue;
  }

  public int getIntValue() {
    return intValue;
  }

  public long getLongValue() {
    return longValue;
  }
}
