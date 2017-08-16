/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class TimeWindow(
    timeColumn: Expression,
    windowDuration: Long,
    slideDuration: Long,
    startTime: Long) extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  //////////////////////////
  // SQL Constructors
  //////////////////////////

  def this(
      timeColumn: Expression,
      windowDuration: Expression,
      slideDuration: Expression,
      startTime: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), TimeWindow.parseExpression(startTime))
  }

  def this(timeColumn: Expression, windowDuration: Expression, slideDuration: Expression) = {
    this(timeColumn, TimeWindow.parseExpression(windowDuration),
      TimeWindow.parseExpression(slideDuration), 0)
  }

  def this(timeColumn: Expression, windowDuration: Expression) = {
    this(timeColumn, windowDuration, windowDuration)
  }

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  /**
   * Validate the inputs for the window duration, slide duration, and start time in addition to
   * the input data type.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (windowDuration <= 0) {
        return TypeCheckFailure(s"The window duration ($windowDuration) must be greater than 0.")
      }
      if (slideDuration <= 0) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be greater than 0.")
      }
      if (startTime < 0) {
        return TypeCheckFailure(s"The start time ($startTime) must be greater than or equal to 0.")
      }
      if (slideDuration > windowDuration) {
        return TypeCheckFailure(s"The slide duration ($slideDuration) must be less than or equal" +
          s" to the windowDuration ($windowDuration).")
      }
      if (startTime >= slideDuration) {
        return TypeCheckFailure(s"The start time ($startTime) must be less than the " +
          s"slideDuration ($slideDuration).")
      }
    }
    dataTypeCheck
  }
}

object TimeWindow {
  /**
   * Parses the interval string for a valid time duration. CalendarInterval expects interval
   * strings to start with the string `interval`. For usability, we prepend `interval` to the string
   * if the user omitted it.
   *
   * @param interval The interval string
   * @return The interval duration in microseconds. SparkSQL casts TimestampType has microsecond
   *         precision.
   */
  private def getIntervalInMicroSeconds(interval: String): Long = {
    if (StringUtils.isBlank(interval)) {
      throw new IllegalArgumentException(
        "The window duration, slide duration and start time cannot be null or blank.")
    }
    val intervalString = if (interval.startsWith("interval")) {
      interval
    } else {
      "interval " + interval
    }
    val cal = CalendarInterval.fromString(intervalString)
    if (cal == null) {
      throw new IllegalArgumentException(
        s"The provided interval ($interval) did not correspond to a valid interval string.")
    }
    if (cal.months > 0) {
      throw new IllegalArgumentException(
        s"Intervals greater than a month is not supported ($interval).")
    }
    cal.microseconds
  }

  /**
   * Parses the duration expression to generate the long value for the original constructor so
   * that we can use `window` in SQL.
   */
  private def parseExpression(expr: Expression): Long = expr match {
    case NonNullLiteral(s, StringType) => getIntervalInMicroSeconds(s.toString)
    case IntegerLiteral(i) => i.toLong
    case NonNullLiteral(l, LongType) => l.toString.toLong
    case _ => throw new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }

  def apply(
      timeColumn: Expression,
      windowDuration: String,
      slideDuration: String,
      startTime: String): TimeWindow = {
    TimeWindow(timeColumn,
      getIntervalInMicroSeconds(windowDuration),
      getIntervalInMicroSeconds(slideDuration),
      getIntervalInMicroSeconds(startTime))
  }
}

/**
 * Expression used internally to convert the TimestampType to Long without losing
 * precision, i.e. in microseconds. Used in time windowing.
 */
case class PreciseTimestamp(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = LongType
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ev.copy(code = eval.code +
      s"""boolean ${ev.isNull} = ${eval.isNull};
         |${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
       """.stripMargin)
  }
}
