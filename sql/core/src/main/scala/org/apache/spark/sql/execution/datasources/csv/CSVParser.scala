/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.csv

import java.io.{CharArrayWriter, StringReader}

import com.univocity.parsers.csv._

import org.apache.spark.internal.Logging

/**
 * Read and parse CSV-like input
 *
 * @param params Parameters object
 */
private[csv] class CsvReader(params: CSVOptions) {

  private val parser: CsvParser = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(params.delimiter)
    format.setQuote(params.quote)
    format.setQuoteEscape(params.escape)
    format.setComment(params.comment)
    settings.setIgnoreLeadingWhitespaces(params.ignoreLeadingWhiteSpaceFlag)
    settings.setIgnoreTrailingWhitespaces(params.ignoreTrailingWhiteSpaceFlag)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(params.inputBufferSize)
    settings.setMaxColumns(params.maxColumns)
    settings.setNullValue(params.nullValue)
    settings.setMaxCharsPerColumn(params.maxCharsPerColumn)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER)

    new CsvParser(settings)
  }

  /**
   * parse a line
   *
   * @param line a String with no newline at the end
   * @return array of strings where each string is a field in the CSV record
   */
  def parseLine(line: String): Array[String] = parser.parseLine(line)
}

/**
 * Converts a sequence of string to CSV string
 *
 * @param params Parameters object for configuration
 * @param headers headers for columns
 */
private[csv] class LineCsvWriter(params: CSVOptions, headers: Seq[String]) extends Logging {
  private val writerSettings = new CsvWriterSettings
  private val format = writerSettings.getFormat

  format.setDelimiter(params.delimiter)
  format.setQuote(params.quote)
  format.setQuoteEscape(params.escape)
  format.setComment(params.comment)

  writerSettings.setNullValue(params.nullValue)
  writerSettings.setEmptyValue(params.nullValue)
  writerSettings.setSkipEmptyLines(true)
  writerSettings.setQuoteAllFields(params.quoteAll)
  writerSettings.setHeaders(headers: _*)
  writerSettings.setQuoteEscapingEnabled(params.escapeQuotes)

  private val buffer = new CharArrayWriter()
  private val writer = new CsvWriter(buffer, writerSettings)

  def writeRow(row: Seq[String], includeHeader: Boolean): Unit = {
    if (includeHeader) {
      writer.writeHeaders()
    }
    writer.writeRow(row.toArray: _*)
  }

  def flush(): String = {
    writer.flush()
    val lines = buffer.toString.stripLineEnd
    buffer.reset()
    lines
  }

  def close(): Unit = {
    writer.close()
  }
}
