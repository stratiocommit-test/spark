/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class GenericAvroSerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val schema : Schema = SchemaBuilder
    .record("testRecord").fields()
    .requiredString("data")
    .endRecord()
  val record = new Record(schema)
  record.put("data", "test data")

  test("schema compression and decompression") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    assert(schema === genericSer.decompress(ByteBuffer.wrap(genericSer.compress(schema))))
  }

  test("record serialization and deserialization") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSer.serializeDatum(record, output)
    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSer.deserializeDatum(input) === record)
  }

  test("uses schema fingerprint to decrease message size") {
    val genericSerFull = new GenericAvroSerializer(conf.getAvroSchema)

    val output = new Output(new ByteArrayOutputStream())

    val beginningNormalPosition = output.total()
    genericSerFull.serializeDatum(record, output)
    output.flush()
    val normalLength = output.total - beginningNormalPosition

    conf.registerAvroSchemas(schema)
    val genericSerFinger = new GenericAvroSerializer(conf.getAvroSchema)
    val beginningFingerprintPosition = output.total()
    genericSerFinger.serializeDatum(record, output)
    val fingerprintLength = output.total - beginningFingerprintPosition

    assert(fingerprintLength < normalLength)
  }

  test("caches previously seen schemas") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    val compressedSchema = genericSer.compress(schema)
    val decompressedSchema = genericSer.decompress(ByteBuffer.wrap(compressedSchema))

    assert(compressedSchema.eq(genericSer.compress(schema)))
    assert(decompressedSchema.eq(genericSer.decompress(ByteBuffer.wrap(compressedSchema))))
  }
}
