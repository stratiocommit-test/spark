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

import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;

/**
 * RowBasedSet
 */
public class RowBasedSet implements RowSet {

  private long startOffset;

  private final Type[] types; // non-null only for writing (server-side)
  private final RemovableList<TRow> rows;

  public RowBasedSet(TableSchema schema) {
    types = schema.toTypes();
    rows = new RemovableList<TRow>();
  }

  public RowBasedSet(TRowSet tRowSet) {
    types = null;
    rows = new RemovableList<TRow>(tRowSet.getRows());
    startOffset = tRowSet.getStartRowOffset();
  }

  private RowBasedSet(Type[] types, List<TRow> rows, long startOffset) {
    this.types = types;
    this.rows = new RemovableList<TRow>(rows);
    this.startOffset = startOffset;
  }

  @Override
  public RowBasedSet addRow(Object[] fields) {
    TRow tRow = new TRow();
    for (int i = 0; i < fields.length; i++) {
      tRow.addToColVals(ColumnValue.toTColumnValue(types[i], fields[i]));
    }
    rows.add(tRow);
    return this;
  }

  @Override
  public int numColumns() {
    return rows.isEmpty() ? 0 : rows.get(0).getColVals().size();
  }

  @Override
  public int numRows() {
    return rows.size();
  }

  public RowBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);
    RowBasedSet result = new RowBasedSet(types, rows.subList(0, numRows), startOffset);
    rows.removeRange(0, numRows);
    startOffset += numRows;
    return result;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public int getSize() {
    return rows.size();
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet();
    tRowSet.setStartRowOffset(startOffset);
    tRowSet.setRows(new ArrayList<TRow>(rows));
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      final Iterator<TRow> iterator = rows.iterator();
      final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object[] next() {
        TRow row = iterator.next();
        List<TColumnValue> values = row.getColVals();
        for (int i = 0; i < values.size(); i++) {
          convey[i] = ColumnValue.toColumnValue(values.get(i));
        }
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  private static class RemovableList<E> extends ArrayList<E> {
    RemovableList() { super(); }
    RemovableList(List<E> rows) { super(rows); }
    @Override
    public void removeRange(int fromIndex, int toIndex) {
      super.removeRange(fromIndex, toIndex);
    }
  }
}
