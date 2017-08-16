/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.parser

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.types._

/**
 * Parser that turns case class strings into datatypes. This is only here to maintain compatibility
 * with Parquet files written by Spark 1.1 and below.
 */
object LegacyTypeStringParser extends RegexParsers {

  protected lazy val primitiveType: Parser[DataType] =
    ( "StringType" ^^^ StringType
      | "FloatType" ^^^ FloatType
      | "IntegerType" ^^^ IntegerType
      | "ByteType" ^^^ ByteType
      | "ShortType" ^^^ ShortType
      | "DoubleType" ^^^ DoubleType
      | "LongType" ^^^ LongType
      | "BinaryType" ^^^ BinaryType
      | "BooleanType" ^^^ BooleanType
      | "DateType" ^^^ DateType
      | "DecimalType()" ^^^ DecimalType.USER_DEFAULT
      | fixedDecimalType
      | "TimestampType" ^^^ TimestampType
      )

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("DecimalType(" ~> "[0-9]+".r) ~ ("," ~> "[0-9]+".r <~ ")") ^^ {
      case precision ~ scale => DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "ArrayType" ~> "(" ~> dataType ~ "," ~ boolVal <~ ")" ^^ {
      case tpe ~ _ ~ containsNull => ArrayType(tpe, containsNull)
    }

  protected lazy val mapType: Parser[DataType] =
    "MapType" ~> "(" ~> dataType ~ "," ~ dataType ~ "," ~ boolVal <~ ")" ^^ {
      case t1 ~ _ ~ t2 ~ _ ~ valueContainsNull => MapType(t1, t2, valueContainsNull)
    }

  protected lazy val structField: Parser[StructField] =
    ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
      case name ~ tpe ~ nullable =>
        StructField(name, tpe, nullable = nullable)
    }

  protected lazy val boolVal: Parser[Boolean] =
    ( "true" ^^^ true
      | "false" ^^^ false
      )

  protected lazy val structType: Parser[DataType] =
    "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ {
      case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    ( arrayType
      | mapType
      | structType
      | primitiveType
      )

  /**
   * Parses a string representation of a DataType.
   */
  def parse(asString: String): DataType = parseAll(dataType, asString) match {
    case Success(result, _) => result
    case failure: NoSuccess =>
      throw new IllegalArgumentException(s"Unsupported dataType: $asString, $failure")
  }
}
