/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.thriftserver

private[hive] object ReflectionUtils {
  def setSuperField(obj : Object, fieldName: String, fieldValue: Object) {
    setAncestorField(obj, 1, fieldName, fieldValue)
  }

  def setAncestorField(obj: AnyRef, level: Int, fieldName: String, fieldValue: AnyRef) {
    val ancestor = Iterator.iterate[Class[_]](obj.getClass)(_.getSuperclass).drop(level).next()
    val field = ancestor.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.set(obj, fieldValue)
  }

  def getSuperField[T](obj: AnyRef, fieldName: String): T = {
    getAncestorField[T](obj, 1, fieldName)
  }

  def getAncestorField[T](clazz: Object, level: Int, fieldName: String): T = {
    val ancestor = Iterator.iterate[Class[_]](clazz.getClass)(_.getSuperclass).drop(level).next()
    val field = ancestor.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(clazz).asInstanceOf[T]
  }

  def invokeStatic(clazz: Class[_], methodName: String, args: (Class[_], AnyRef)*): AnyRef = {
    invoke(clazz, null, methodName, args: _*)
  }

  def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {

    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }
}
