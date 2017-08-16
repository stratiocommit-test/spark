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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.service.cli.thrift.TCLIServiceConstants;
import org.apache.hive.service.cli.thrift.TTypeQualifierValue;
import org.apache.hive.service.cli.thrift.TTypeQualifiers;

/**
 * This class holds type qualifier information for a primitive type,
 * such as char/varchar length or decimal precision/scale.
 */
public class TypeQualifiers {
  private Integer characterMaximumLength;
  private Integer precision;
  private Integer scale;

  public TypeQualifiers() {}

  public Integer getCharacterMaximumLength() {
    return characterMaximumLength;
  }
  public void setCharacterMaximumLength(int characterMaximumLength) {
    this.characterMaximumLength = characterMaximumLength;
  }

  public TTypeQualifiers toTTypeQualifiers() {
    TTypeQualifiers ret = null;

    Map<String, TTypeQualifierValue> qMap = new HashMap<String, TTypeQualifierValue>();
    if (getCharacterMaximumLength() != null) {
      TTypeQualifierValue val = new TTypeQualifierValue();
      val.setI32Value(getCharacterMaximumLength().intValue());
      qMap.put(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH, val);
    }

    if (precision != null) {
      TTypeQualifierValue val = new TTypeQualifierValue();
      val.setI32Value(precision.intValue());
      qMap.put(TCLIServiceConstants.PRECISION, val);
    }

    if (scale != null) {
      TTypeQualifierValue val = new TTypeQualifierValue();
      val.setI32Value(scale.intValue());
      qMap.put(TCLIServiceConstants.SCALE, val);
    }

    if (qMap.size() > 0) {
      ret = new TTypeQualifiers(qMap);
    }

    return ret;
  }

  public static TypeQualifiers fromTTypeQualifiers(TTypeQualifiers ttq) {
    TypeQualifiers ret = null;
    if (ttq != null) {
      ret = new TypeQualifiers();
      Map<String, TTypeQualifierValue> tqMap = ttq.getQualifiers();

      if (tqMap.containsKey(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH)) {
        ret.setCharacterMaximumLength(
            tqMap.get(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH).getI32Value());
      }

      if (tqMap.containsKey(TCLIServiceConstants.PRECISION)) {
        ret.setPrecision(tqMap.get(TCLIServiceConstants.PRECISION).getI32Value());
      }

      if (tqMap.containsKey(TCLIServiceConstants.SCALE)) {
        ret.setScale(tqMap.get(TCLIServiceConstants.SCALE).getI32Value());
      }
    }
    return ret;
  }

  public static TypeQualifiers fromTypeInfo(PrimitiveTypeInfo pti) {
    TypeQualifiers result = null;
    if (pti instanceof VarcharTypeInfo) {
      result = new TypeQualifiers();
      result.setCharacterMaximumLength(((VarcharTypeInfo)pti).getLength());
    }  else if (pti instanceof CharTypeInfo) {
      result = new TypeQualifiers();
      result.setCharacterMaximumLength(((CharTypeInfo)pti).getLength());
    } else if (pti instanceof DecimalTypeInfo) {
      result = new TypeQualifiers();
      result.setPrecision(((DecimalTypeInfo)pti).precision());
      result.setScale(((DecimalTypeInfo)pti).scale());
    }
    return result;
  }

  public Integer getPrecision() {
    return precision;
  }

  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  public Integer getScale() {
    return scale;
  }

  public void setScale(Integer scale) {
    this.scale = scale;
  }

}
