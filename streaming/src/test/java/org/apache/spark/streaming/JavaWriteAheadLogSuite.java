/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.streaming.util.WriteAheadLog;
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle;
import org.apache.spark.streaming.util.WriteAheadLogUtils;

import org.junit.Test;
import org.junit.Assert;

public class JavaWriteAheadLogSuite extends WriteAheadLog {

  static class JavaWriteAheadLogSuiteHandle extends WriteAheadLogRecordHandle {
    int index = -1;
    JavaWriteAheadLogSuiteHandle(int idx) {
      index = idx;
    }
  }

  static class Record {
    long time;
    int index;
    ByteBuffer buffer;

    Record(long tym, int idx, ByteBuffer buf) {
      index = idx;
      time = tym;
      buffer = buf;
    }
  }
  private int index = -1;
  private final List<Record> records = new ArrayList<>();


  // Methods for WriteAheadLog
  @Override
  public WriteAheadLogRecordHandle write(ByteBuffer record, long time) {
    index += 1;
    records.add(new Record(time, index, record));
    return new JavaWriteAheadLogSuiteHandle(index);
  }

  @Override
  public ByteBuffer read(WriteAheadLogRecordHandle handle) {
    if (handle instanceof JavaWriteAheadLogSuiteHandle) {
      int reqdIndex = ((JavaWriteAheadLogSuiteHandle) handle).index;
      for (Record record: records) {
        if (record.index == reqdIndex) {
          return record.buffer;
        }
      }
    }
    return null;
  }

  @Override
  public Iterator<ByteBuffer> readAll() {
    return Iterators.transform(records.iterator(), new Function<Record,ByteBuffer>() {
      @Override
      public ByteBuffer apply(Record input) {
        return input.buffer;
      }
    });
  }

  @Override
  public void clean(long threshTime, boolean waitForCompletion) {
    for (int i = 0; i < records.size(); i++) {
      if (records.get(i).time < threshTime) {
        records.remove(i);
        i--;
      }
    }
  }

  @Override
  public void close() {
    records.clear();
  }

  @Test
  public void testCustomWAL() {
    SparkConf conf = new SparkConf();
    conf.set("spark.streaming.driver.writeAheadLog.class", JavaWriteAheadLogSuite.class.getName());
    conf.set("spark.streaming.driver.writeAheadLog.allowBatching", "false");
    WriteAheadLog wal = WriteAheadLogUtils.createLogForDriver(conf, null, null);

    String data1 = "data1";
    WriteAheadLogRecordHandle handle = wal.write(JavaUtils.stringToBytes(data1), 1234);
    Assert.assertTrue(handle instanceof JavaWriteAheadLogSuiteHandle);
    Assert.assertEquals(JavaUtils.bytesToString(wal.read(handle)), data1);

    wal.write(JavaUtils.stringToBytes("data2"), 1235);
    wal.write(JavaUtils.stringToBytes("data3"), 1236);
    wal.write(JavaUtils.stringToBytes("data4"), 1237);
    wal.clean(1236, false);

    Iterator<ByteBuffer> dataIterator = wal.readAll();
    List<String> readData = new ArrayList<>();
    while (dataIterator.hasNext()) {
      readData.add(JavaUtils.bytesToString(dataIterator.next()));
    }
    Assert.assertEquals(readData, Arrays.asList("data3", "data4"));
  }
}
