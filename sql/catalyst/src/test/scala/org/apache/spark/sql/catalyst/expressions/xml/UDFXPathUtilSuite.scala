/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.xml

import javax.xml.xpath.XPathConstants.STRING

import org.w3c.dom.Node
import org.w3c.dom.NodeList

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[UDFXPathUtil]]. Loosely based on Hive's TestUDFXPathUtil.java.
 */
class UDFXPathUtilSuite extends SparkFunSuite {

  private lazy val util = new UDFXPathUtil

  test("illegal arguments") {
    // null args
    assert(util.eval(null, "a/text()", STRING) == null)
    assert(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", null, STRING) == null)
    assert(
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text()", null) == null)

    // empty String args
    assert(util.eval("", "a/text()", STRING) == null)
    assert(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "", STRING) == null)

    // wrong expression:
    intercept[RuntimeException] {
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text(", STRING)
    }
  }

  test("generic eval") {
    val ret =
      util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/c[2]/text()", STRING)
    assert(ret == "c2")
  }

  test("boolean eval") {
    var ret =
      util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[1]/text()")
    assert(ret == true)

    ret = util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]")
    assert(ret == false)
  }

  test("string eval") {
    var ret =
      util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[3]/text()")
    assert(ret == "b3")

    ret =
      util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]/text()")
    assert(ret == "")

    ret = util.evalString(
      "<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k")
    assert(ret == "foo")
  }

  test("number eval") {
    var ret =
      util.evalNumber("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]")
    assert(ret == -77.0d)

    ret = util.evalNumber(
      "<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k")
    assert(ret.isNaN)
  }

  test("node eval") {
    val ret = util.evalNode("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]")
    assert(ret != null && ret.isInstanceOf[Node])
  }

  test("node list eval") {
    val ret = util.evalNodeList("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/*")
    assert(ret != null && ret.isInstanceOf[NodeList])
    assert(ret.asInstanceOf[NodeList].getLength == 5)
  }
}
