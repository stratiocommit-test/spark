/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import org.scalatest.{FunSpec, Matchers}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._

class RowTest extends FunSpec with Matchers {

  val schema = StructType(
    StructField("col1", StringType) ::
    StructField("col2", StringType) ::
    StructField("col3", IntegerType) :: Nil)
  val values = Array("value1", "value2", 1)
  val valuesWithoutCol3 = Array[Any](null, "value2", null)

  val sampleRow: Row = new GenericRowWithSchema(values, schema)
  val sampleRowWithoutCol3: Row = new GenericRowWithSchema(valuesWithoutCol3, schema)
  val noSchemaRow: Row = new GenericRow(values)

  describe("Row (without schema)") {
    it("throws an exception when accessing by fieldName") {
      intercept[UnsupportedOperationException] {
        noSchemaRow.fieldIndex("col1")
      }
      intercept[UnsupportedOperationException] {
        noSchemaRow.getAs("col1")
      }
    }
  }

  describe("Row (with schema)") {
    it("fieldIndex(name) returns field index") {
      sampleRow.fieldIndex("col1") shouldBe 0
      sampleRow.fieldIndex("col3") shouldBe 2
    }

    it("getAs[T] retrieves a value by fieldname") {
      sampleRow.getAs[String]("col1") shouldBe "value1"
      sampleRow.getAs[Int]("col3") shouldBe 1
    }

    it("Accessing non existent field throws an exception") {
      intercept[IllegalArgumentException] {
        sampleRow.getAs[String]("non_existent")
      }
    }

    it("getValuesMap() retrieves values of multiple fields as a Map(field -> value)") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> "value2"
      )
      sampleRow.getValuesMap(List("col1", "col2")) shouldBe expected
    }

    it("getValuesMap() retrieves null value on non AnyVal Type") {
      val expected = Map(
        "col1" -> null,
        "col2" -> "value2"
      )
      sampleRowWithoutCol3.getValuesMap[String](List("col1", "col2")) shouldBe expected
    }

    it("getAs() on type extending AnyVal throws an exception when accessing field that is null") {
      intercept[NullPointerException] {
        sampleRowWithoutCol3.getInt(sampleRowWithoutCol3.fieldIndex("col3"))
      }
    }

    it("getAs() on type extending AnyVal does not throw exception when value is null") {
      sampleRowWithoutCol3.getAs[String](sampleRowWithoutCol3.fieldIndex("col1")) shouldBe null
    }
  }

  describe("row equals") {
    val externalRow = Row(1, 2)
    val externalRow2 = Row(1, 2)
    val internalRow = InternalRow(1, 2)
    val internalRow2 = InternalRow(1, 2)

    it("equality check for external rows") {
      externalRow shouldEqual externalRow2
    }

    it("equality check for internal rows") {
      internalRow shouldEqual internalRow2
    }
  }

  describe("row immutability") {
    val values = Seq(1, 2, "3", "IV", 6L)
    val externalRow = Row.fromSeq(values)
    val internalRow = InternalRow.fromSeq(values)

    def modifyValues(values: Seq[Any]): Seq[Any] = {
      val array = values.toArray
      array(2) = "42"
      array
    }

    it("copy should return same ref for external rows") {
      externalRow should be theSameInstanceAs externalRow.copy()
    }

    it("copy should return same ref for internal rows") {
      internalRow should be theSameInstanceAs internalRow.copy()
    }

    it("toSeq should not expose internal state for external rows") {
      val modifiedValues = modifyValues(externalRow.toSeq)
      externalRow.toSeq should not equal modifiedValues
    }

    it("toSeq should not expose internal state for internal rows") {
      val modifiedValues = modifyValues(internalRow.toSeq(Seq.empty))
      internalRow.toSeq(Seq.empty) should not equal modifiedValues
    }
  }
}
