/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Locale, TimeZone}
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

private[v1] class SimpleDateParam(val originalValue: String) {

  val timestamp: Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz", Locale.US)
    try {
      format.parse(originalValue).getTime()
    } catch {
      case _: ParseException =>
        val gmtDay = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
        gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))
        try {
          gmtDay.parse(originalValue).getTime()
        } catch {
          case _: ParseException =>
            throw new WebApplicationException(
              Response
                .status(Status.BAD_REQUEST)
                .entity("Couldn't parse date: " + originalValue)
                .build()
            )
        }
    }
  }
}
