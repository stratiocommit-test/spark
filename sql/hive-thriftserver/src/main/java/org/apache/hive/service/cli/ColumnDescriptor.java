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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.thrift.TColumnDesc;


/**
 * ColumnDescriptor.
 *
 */
public class ColumnDescriptor {
  private final String name;
  private final String comment;
  private final TypeDescriptor type;
  // ordinal position of this column in the schema
  private final int position;

  public ColumnDescriptor(String name, String comment, TypeDescriptor type, int position) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.position = position;
  }

  public ColumnDescriptor(TColumnDesc tColumnDesc) {
    name = tColumnDesc.getColumnName();
    comment = tColumnDesc.getComment();
    type = new TypeDescriptor(tColumnDesc.getTypeDesc());
    position = tColumnDesc.getPosition();
  }

  public ColumnDescriptor(FieldSchema column, int position) {
    name = column.getName();
    comment = column.getComment();
    type = new TypeDescriptor(column.getType());
    this.position = position;
  }

  public static ColumnDescriptor newPrimitiveColumnDescriptor(String name, String comment, Type type, int position) {
    // Current usage looks like it's only for metadata columns, but if that changes then
    // this method may need to require a type qualifiers aruments.
    return new ColumnDescriptor(name, comment, new TypeDescriptor(type), position);
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public TypeDescriptor getTypeDescriptor() {
    return type;
  }

  public int getOrdinalPosition() {
    return position;
  }

  public TColumnDesc toTColumnDesc() {
    TColumnDesc tColumnDesc = new TColumnDesc();
    tColumnDesc.setColumnName(name);
    tColumnDesc.setComment(comment);
    tColumnDesc.setTypeDesc(type.toTTypeDesc());
    tColumnDesc.setPosition(position);
    return tColumnDesc;
  }

  public Type getType() {
    return type.getType();
  }

  public boolean isPrimitive() {
    return type.getType().isPrimitiveType();
  }

  public String getTypeName() {
    return type.getTypeName();
  }
}
