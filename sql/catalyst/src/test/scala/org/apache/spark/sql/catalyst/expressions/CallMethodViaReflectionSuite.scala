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

import java.sql.Timestamp

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.types.{IntegerType, StringType}

/** A static class for testing purpose. */
object ReflectStaticClass {
  def method1(): String = "m1"
  def method2(v1: Int): String = "m" + v1
  def method3(v1: java.lang.Integer): String = "m" + v1
  def method4(v1: Int, v2: String): String = "m" + v1 + v2
}

/** A non-static class for testing purpose. */
class ReflectDynamicClass {
  def method1(): String = "m1"
}

/**
 * Test suite for [[CallMethodViaReflection]] and its companion object.
 */
class CallMethodViaReflectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  import CallMethodViaReflection._

  // Get rid of the $ so we are getting the companion object's name.
  private val staticClassName = ReflectStaticClass.getClass.getName.stripSuffix("$")
  private val dynamicClassName = classOf[ReflectDynamicClass].getName

  test("findMethod via reflection for static methods") {
    assert(findMethod(staticClassName, "method1", Seq.empty).exists(_.getName == "method1"))
    assert(findMethod(staticClassName, "method2", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method3", Seq(IntegerType)).isDefined)
    assert(findMethod(staticClassName, "method4", Seq(IntegerType, StringType)).isDefined)
  }

  test("findMethod for a JDK library") {
    assert(findMethod(classOf[java.util.UUID].getName, "randomUUID", Seq.empty).isDefined)
  }

  test("class not found") {
    val ret = createExpr("some-random-class", "method").checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("not found") && errorMsg.contains("class"))
  }

  test("method not found because name does not match") {
    val ret = createExpr(staticClassName, "notfoundmethod").checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("cannot find a static method"))
  }

  test("method not found because there is no static method") {
    val ret = createExpr(dynamicClassName, "method1").checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("cannot find a static method"))
  }

  test("input type checking") {
    assert(CallMethodViaReflection(Seq.empty).checkInputDataTypes().isFailure)
    assert(CallMethodViaReflection(Seq(Literal(staticClassName))).checkInputDataTypes().isFailure)
    assert(CallMethodViaReflection(
      Seq(Literal(staticClassName), Literal(1))).checkInputDataTypes().isFailure)
    assert(createExpr(staticClassName, "method1").checkInputDataTypes().isSuccess)
  }

  test("unsupported type checking") {
    val ret = createExpr(staticClassName, "method1", new Timestamp(1)).checkInputDataTypes()
    assert(ret.isFailure)
    val errorMsg = ret.asInstanceOf[TypeCheckFailure].message
    assert(errorMsg.contains("arguments from the third require boolean, byte, short"))
  }

  test("invoking methods using acceptable types") {
    checkEvaluation(createExpr(staticClassName, "method1"), "m1")
    checkEvaluation(createExpr(staticClassName, "method2", 2), "m2")
    checkEvaluation(createExpr(staticClassName, "method3", 3), "m3")
    checkEvaluation(createExpr(staticClassName, "method4", 4, "four"), "m4four")
  }

  private def createExpr(className: String, methodName: String, args: Any*) = {
    CallMethodViaReflection(
      Literal.create(className, StringType) +:
      Literal.create(methodName, StringType) +:
      args.map(Literal.apply)
    )
  }
}
