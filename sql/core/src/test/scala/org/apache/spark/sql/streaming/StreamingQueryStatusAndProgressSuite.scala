/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.streaming

import java.util.UUID

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.StreamingQueryStatusAndProgressSuite._


class StreamingQueryStatusAndProgressSuite extends SparkFunSuite {

  test("StreamingQueryProgress - prettyJson") {
    val json1 = testProgress1.prettyJson
    assert(json1 ===
      s"""
        |{
        |  "id" : "${testProgress1.id.toString}",
        |  "runId" : "${testProgress1.runId.toString}",
        |  "name" : "myName",
        |  "timestamp" : "2016-12-05T20:54:20.827Z",
        |  "numInputRows" : 678,
        |  "inputRowsPerSecond" : 10.0,
        |  "durationMs" : {
        |    "total" : 0
        |  },
        |  "eventTime" : {
        |    "avg" : "2016-12-05T20:54:20.827Z",
        |    "max" : "2016-12-05T20:54:20.827Z",
        |    "min" : "2016-12-05T20:54:20.827Z",
        |    "watermark" : "2016-12-05T20:54:20.827Z"
        |  },
        |  "stateOperators" : [ {
        |    "numRowsTotal" : 0,
        |    "numRowsUpdated" : 1
        |  } ],
        |  "sources" : [ {
        |    "description" : "source",
        |    "startOffset" : 123,
        |    "endOffset" : 456,
        |    "numInputRows" : 678,
        |    "inputRowsPerSecond" : 10.0
        |  } ],
        |  "sink" : {
        |    "description" : "sink"
        |  }
        |}
      """.stripMargin.trim)
    assert(compact(parse(json1)) === testProgress1.json)

    val json2 = testProgress2.prettyJson
    assert(
      json2 ===
        s"""
         |{
         |  "id" : "${testProgress2.id.toString}",
         |  "runId" : "${testProgress2.runId.toString}",
         |  "name" : null,
         |  "timestamp" : "2016-12-05T20:54:20.827Z",
         |  "numInputRows" : 678,
         |  "durationMs" : {
         |    "total" : 0
         |  },
         |  "stateOperators" : [ {
         |    "numRowsTotal" : 0,
         |    "numRowsUpdated" : 1
         |  } ],
         |  "sources" : [ {
         |    "description" : "source",
         |    "startOffset" : 123,
         |    "endOffset" : 456,
         |    "numInputRows" : 678
         |  } ],
         |  "sink" : {
         |    "description" : "sink"
         |  }
         |}
      """.stripMargin.trim)
    assert(compact(parse(json2)) === testProgress2.json)
  }

  test("StreamingQueryProgress - json") {
    assert(compact(parse(testProgress1.json)) === testProgress1.json)
    assert(compact(parse(testProgress2.json)) === testProgress2.json)
  }

  test("StreamingQueryProgress - toString") {
    assert(testProgress1.toString === testProgress1.prettyJson)
    assert(testProgress2.toString === testProgress2.prettyJson)
  }

  test("StreamingQueryStatus - prettyJson") {
    val json = testStatus.prettyJson
    assert(json ===
      """
        |{
        |  "message" : "active",
        |  "isDataAvailable" : true,
        |  "isTriggerActive" : false
        |}
      """.stripMargin.trim)
  }

  test("StreamingQueryStatus - json") {
    assert(compact(parse(testStatus.json)) === testStatus.json)
  }

  test("StreamingQueryStatus - toString") {
    assert(testStatus.toString === testStatus.prettyJson)
  }
}

object StreamingQueryStatusAndProgressSuite {
  val testProgress1 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = "myName",
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    durationMs = Map("total" -> 0L).mapValues(long2Long).asJava,
    eventTime = Map(
      "max" -> "2016-12-05T20:54:20.827Z",
      "min" -> "2016-12-05T20:54:20.827Z",
      "avg" -> "2016-12-05T20:54:20.827Z",
      "watermark" -> "2016-12-05T20:54:20.827Z").asJava,
    stateOperators = Array(new StateOperatorProgress(numRowsTotal = 0, numRowsUpdated = 1)),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        numInputRows = 678,
        inputRowsPerSecond = 10.0,
        processedRowsPerSecond = Double.PositiveInfinity  // should not be present in the json
      )
    ),
    sink = new SinkProgress("sink")
  )

  val testProgress2 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = null, // should not be present in the json
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    durationMs = Map("total" -> 0L).mapValues(long2Long).asJava,
    eventTime = Map.empty[String, String].asJava,  // empty maps should be handled correctly
    stateOperators = Array(new StateOperatorProgress(numRowsTotal = 0, numRowsUpdated = 1)),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        numInputRows = 678,
        inputRowsPerSecond = Double.NaN, // should not be present in the json
        processedRowsPerSecond = Double.NegativeInfinity // should not be present in the json
      )
    ),
    sink = new SinkProgress("sink")
  )

  val testStatus = new StreamingQueryStatus("active", true, false)
}

