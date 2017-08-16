/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * This is based on hive-exec-1.2.1
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat.OrcRecordReader}.
 * This class exposes getObjectInspector which can be used for reducing
 * NameNode calls in OrcRelation.
 */
public class SparkOrcNewRecordReader extends
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> {
  private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
  private final int numColumns;
  OrcStruct value;
  private float progress = 0.0f;
  private ObjectInspector objectInspector;

  public SparkOrcNewRecordReader(Reader file, Configuration conf,
      long offset, long length) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    value = new OrcStruct(numColumns);
    this.reader = OrcInputFormat.createReaderFromFile(file, conf, offset,
        length);
    this.objectInspector = file.getObjectInspector();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public OrcStruct getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return progress;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader.hasNext()) {
      reader.next(value);
      progress = reader.getProgress();
      return true;
    } else {
      return false;
    }
  }

  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
