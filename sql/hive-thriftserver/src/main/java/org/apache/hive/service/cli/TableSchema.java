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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TTableSchema;

/**
 * TableSchema.
 *
 */
public class TableSchema {
  private final List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();

  public TableSchema() {
  }

  public TableSchema(int numColumns) {
    // TODO: remove this constructor
  }

  public TableSchema(TTableSchema tTableSchema) {
    for (TColumnDesc tColumnDesc : tTableSchema.getColumns()) {
      columns.add(new ColumnDescriptor(tColumnDesc));
    }
  }

  public TableSchema(List<FieldSchema> fieldSchemas) {
    int pos = 1;
    for (FieldSchema field : fieldSchemas) {
      columns.add(new ColumnDescriptor(field, pos++));
    }
  }

  public TableSchema(Schema schema) {
    this(schema.getFieldSchemas());
  }

  public List<ColumnDescriptor> getColumnDescriptors() {
    return new ArrayList<ColumnDescriptor>(columns);
  }

  public ColumnDescriptor getColumnDescriptorAt(int pos) {
    return columns.get(pos);
  }

  public int getSize() {
    return columns.size();
  }

  public void clear() {
    columns.clear();
  }


  public TTableSchema toTTableSchema() {
    TTableSchema tTableSchema = new TTableSchema();
    for (ColumnDescriptor col : columns) {
      tTableSchema.addToColumns(col.toTColumnDesc());
    }
    return tTableSchema;
  }

  public Type[] toTypes() {
    Type[] types = new Type[columns.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = columns.get(i).getType();
    }
    return types;
  }

  public TableSchema addPrimitiveColumn(String columnName, Type columnType, String columnComment) {
    columns.add(ColumnDescriptor.newPrimitiveColumnDescriptor(columnName, columnComment, columnType, columns.size() + 1));
    return this;
  }

  public TableSchema addStringColumn(String columnName, String columnComment) {
    columns.add(ColumnDescriptor.newPrimitiveColumnDescriptor(columnName, columnComment, Type.STRING_TYPE, columns.size() + 1));
    return this;
  }
}
