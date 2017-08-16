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
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;

/**
 * ColumnBasedSet.
 */
public class ColumnBasedSet implements RowSet {

  private long startOffset;

  private final Type[] types; // non-null only for writing (server-side)
  private final List<Column> columns;

  public ColumnBasedSet(TableSchema schema) {
    types = schema.toTypes();
    columns = new ArrayList<Column>();
    for (ColumnDescriptor colDesc : schema.getColumnDescriptors()) {
      columns.add(new Column(colDesc.getType()));
    }
  }

  public ColumnBasedSet(TRowSet tRowSet) {
    types = null;
    columns = new ArrayList<Column>();
    for (TColumn tvalue : tRowSet.getColumns()) {
      columns.add(new Column(tvalue));
    }
    startOffset = tRowSet.getStartRowOffset();
  }

  private ColumnBasedSet(Type[] types, List<Column> columns, long startOffset) {
    this.types = types;
    this.columns = columns;
    this.startOffset = startOffset;
  }

  @Override
  public ColumnBasedSet addRow(Object[] fields) {
    for (int i = 0; i < fields.length; i++) {
      columns.get(i).addValue(types[i], fields[i]);
    }
    return this;
  }

  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public int numColumns() {
    return columns.size();
  }

  @Override
  public int numRows() {
    return columns.isEmpty() ? 0 : columns.get(0).size();
  }

  @Override
  public ColumnBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<Column> subset = new ArrayList<Column>();
    for (int i = 0; i < columns.size(); i++) {
      subset.add(columns.get(i).extractSubset(0, numRows));
    }
    ColumnBasedSet result = new ColumnBasedSet(types, subset, startOffset);
    startOffset += numRows;
    return result;
  }

  @Override
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet(startOffset, new ArrayList<TRow>());
    for (int i = 0; i < columns.size(); i++) {
      tRowSet.addToColumns(columns.get(i).toTColumn());
    }
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      private int index;
      private final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return index < numRows();
      }

      @Override
      public Object[] next() {
        for (int i = 0; i < columns.size(); i++) {
          convey[i] = columns.get(i).get(index);
        }
        index++;
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Object[] fill(int index, Object[] convey) {
    for (int i = 0; i < columns.size(); i++) {
      convey[i] = columns.get(i).get(index);
    }
    return convey;
  }
}
