/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.Utils

/**
 * used to test close InputStream in UnsafeRowSerializer
 */
class ClosableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
  var closed: Boolean = false
  override def close(): Unit = {
    closed = true
    super.close()
  }
}

class UnsafeRowSerializerSuite extends SparkFunSuite with LocalSparkContext {

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  test("toUnsafeRow() test helper method") {
    // This currently doesnt work because the generic getter throws an exception.
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    assert(row.getString(0) === unsafeRow.getUTF8String(0).toString)
    assert(row.getInt(1) === unsafeRow.getInt(1))
  }

  test("basic row serialization") {
    val rows = Seq(Row("Hello", 1), Row("World", 2))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)
    for (unsafeRow <- unsafeRows) {
      serializerStream.writeKey(0)
      serializerStream.writeValue(unsafeRow)
    }
    serializerStream.close()
    val input = new ClosableByteArrayInputStream(baos.toByteArray)
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    for (expectedRow <- unsafeRows) {
      val actualRow = deserializerIter.next().asInstanceOf[(Integer, UnsafeRow)]._2
      assert(expectedRow.getSizeInBytes === actualRow.getSizeInBytes)
      assert(expectedRow.getString(0) === actualRow.getString(0))
      assert(expectedRow.getInt(1) === actualRow.getInt(1))
    }
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }

  test("close empty input stream") {
    val input = new ClosableByteArrayInputStream(Array.empty)
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }

  test("SPARK-10466: external sorter spilling with unsafe row serializer") {
    var sc: SparkContext = null
    var outputFile: File = null
    val oldEnv = SparkEnv.get // save the old SparkEnv, as it will be overwritten
    Utils.tryWithSafeFinally {
      val conf = new SparkConf()
        .set("spark.shuffle.spill.initialMemoryThreshold", "1")
        .set("spark.shuffle.sort.bypassMergeThreshold", "0")
        .set("spark.testing.memory", "80000")

      sc = new SparkContext("local", "test", conf)
      outputFile = File.createTempFile("test-unsafe-row-serializer-spill", "")
      // prepare data
      val converter = unsafeRowConverter(Array(IntegerType))
      val data = (1 to 10000).iterator.map { i =>
        (i, converter(Row(i)))
      }
      val taskMemoryManager = new TaskMemoryManager(sc.env.memoryManager, 0)
      val taskContext = new TaskContextImpl(0, 0, 0, 0, taskMemoryManager, new Properties, null)

      val sorter = new ExternalSorter[Int, UnsafeRow, UnsafeRow](
        taskContext,
        partitioner = Some(new HashPartitioner(10)),
        serializer = new UnsafeRowSerializer(numFields = 1))

      // Ensure we spilled something and have to merge them later
      assert(sorter.numSpills === 0)
      sorter.insertAll(data)
      assert(sorter.numSpills > 0)

      // Merging spilled files should not throw assertion error
      sorter.writePartitionedFile(ShuffleBlockId(0, 0, 0), outputFile)
    } {
      // Clean up
      if (sc != null) {
        sc.stop()
      }

      // restore the spark env
      SparkEnv.set(oldEnv)

      if (outputFile != null) {
        outputFile.delete()
      }
    }
  }

  test("SPARK-10403: unsafe row serializer with SortShuffleManager") {
    val conf = new SparkConf().set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local", "test", conf)
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    val rowsRDD = sc.parallelize(Seq((0, unsafeRow), (1, unsafeRow), (0, unsafeRow)))
      .asInstanceOf[RDD[Product2[Int, InternalRow]]]
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rowsRDD,
        new PartitionIdPassthrough(2),
        new UnsafeRowSerializer(2))
    val shuffled = new ShuffledRowRDD(dependency)
    shuffled.count()
  }
}
