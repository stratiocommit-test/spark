/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.annotation

import scala.annotation.StaticAnnotation
import scala.annotation.meta._

/**
 * A Scala annotation that specifies the Spark version when a definition was added.
 * Different from the `@since` tag in JavaDoc, this annotation does not require explicit JavaDoc and
 * hence works for overridden methods that inherit API documentation directly from parents.
 * The limitation is that it does not show up in the generated Java API documentation.
 */
@param @field @getter @setter @beanGetter @beanSetter
private[spark] class Since(version: String) extends StaticAnnotation
