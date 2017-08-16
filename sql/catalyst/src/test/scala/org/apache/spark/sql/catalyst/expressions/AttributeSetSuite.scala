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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.IntegerType

class AttributeSetSuite extends SparkFunSuite {

  val aUpper = AttributeReference("A", IntegerType)(exprId = ExprId(1))
  val aLower = AttributeReference("a", IntegerType)(exprId = ExprId(1))
  val fakeA = AttributeReference("a", IntegerType)(exprId = ExprId(3))
  val aSet = AttributeSet(aLower :: Nil)

  val bUpper = AttributeReference("B", IntegerType)(exprId = ExprId(2))
  val bLower = AttributeReference("b", IntegerType)(exprId = ExprId(2))
  val bSet = AttributeSet(bUpper :: Nil)

  val aAndBSet = AttributeSet(aUpper :: bUpper :: Nil)

  test("sanity check") {
    assert(aUpper != aLower)
    assert(bUpper != bLower)
  }

  test("checks by id not name") {
    assert(aSet.contains(aUpper) === true)
    assert(aSet.contains(aLower) === true)
    assert(aSet.contains(fakeA) === false)

    assert(aSet.contains(bUpper) === false)
    assert(aSet.contains(bLower) === false)
  }

  test("++ preserves AttributeSet")  {
    assert((aSet ++ bSet).contains(aUpper) === true)
    assert((aSet ++ bSet).contains(aLower) === true)
  }

  test("extracts all references references") {
    val addSet = AttributeSet(Add(aUpper, Alias(bUpper, "test")()):: Nil)
    assert(addSet.contains(aUpper))
    assert(addSet.contains(aLower))
    assert(addSet.contains(bUpper))
    assert(addSet.contains(bLower))
  }

  test("dedups attributes") {
    assert(AttributeSet(aUpper :: aLower :: Nil).size === 1)
  }

  test("subset") {
    assert(aSet.subsetOf(aAndBSet) === true)
    assert(aAndBSet.subsetOf(aSet) === false)
  }

  test("equality") {
    assert(aSet != aAndBSet)
    assert(aAndBSet != aSet)
    assert(aSet != bSet)
    assert(bSet != aSet)

    assert(aSet == aSet)
    assert(aSet == AttributeSet(aUpper :: Nil))
  }
}
