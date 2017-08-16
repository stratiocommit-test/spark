/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.core.{JsonParser, JsonToken}

import org.apache.spark.sql.types._

object JacksonUtils {
  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  /**
   * Verify if the schema is supported in JSON parsing.
   */
  def verifySchema(schema: StructType): Unit = {
    def verifyType(name: String, dataType: DataType): Unit = dataType match {
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | StringType | TimestampType | DateType | BinaryType | _: DecimalType =>

      case st: StructType => st.foreach(field => verifyType(field.name, field.dataType))

      case at: ArrayType => verifyType(name, at.elementType)

      case mt: MapType => verifyType(name, mt.keyType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert column $name of type ${dataType.simpleString} to JSON.")
    }

    schema.foreach(field => verifyType(field.name, field.dataType))
  }
}
