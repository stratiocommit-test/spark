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

import java.io.OutputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, SimpleTimeZone}
import javax.ws.rs.Produces
import javax.ws.rs.core.{MediaType, MultivaluedMap}
import javax.ws.rs.ext.{MessageBodyWriter, Provider}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

/**
 * This class converts the POJO metric responses into json, using jackson.
 *
 * This doesn't follow the standard jersey-jackson plugin options, because we want to stick
 * with an old version of jersey (since we have it from yarn anyway) and don't want to pull in lots
 * of dependencies from a new plugin.
 *
 * Note that jersey automatically discovers this class based on its package and its annotations.
 */
@Provider
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class JacksonMessageWriter extends MessageBodyWriter[Object]{

  val mapper = new ObjectMapper() {
    override def writeValueAsString(t: Any): String = {
      super.writeValueAsString(t)
    }
  }
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)

  override def isWriteable(
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Boolean = {
      true
  }

  override def writeTo(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType,
      multivaluedMap: MultivaluedMap[String, AnyRef],
      outputStream: OutputStream): Unit = {
    t match {
      case ErrorWrapper(err) => outputStream.write(err.getBytes(StandardCharsets.UTF_8))
      case _ => mapper.writeValue(outputStream, t)
    }
  }

  override def getSize(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Long = {
    -1L
  }
}

private[spark] object JacksonMessageWriter {
  def makeISODateFormat: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'", Locale.US)
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }
}
