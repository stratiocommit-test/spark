/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.source.libsvm

/**
 * `libsvm` package implements Spark SQL data source API for loading LIBSVM data as `DataFrame`.
 * The loaded `DataFrame` has two columns: `label` containing labels stored as doubles and
 * `features` containing feature vectors stored as `Vector`s.
 *
 * To use LIBSVM data source, you need to set "libsvm" as the format in `DataFrameReader` and
 * optionally specify options, for example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("libsvm")
 *     .option("numFeatures", "780")
 *     .load("data/mllib/sample_libsvm_data.txt")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("libsvm")
 *     .option("numFeatures, "780")
 *     .load("data/mllib/sample_libsvm_data.txt");
 * }}}
 *
 * LIBSVM data source supports the following options:
 *  - "numFeatures": number of features.
 *    If unspecified or nonpositive, the number of features will be determined automatically at the
 *    cost of one additional pass.
 *    This is also useful when the dataset is already split into multiple files and you want to load
 *    them separately, because some features may not present in certain files, which leads to
 *    inconsistent feature dimensions.
 *  - "vectorType": feature vector type, "sparse" (default) or "dense".
 *
 * @note This class is public for documentation purpose. Please don't use this class directly.
 * Rather, use the data source API as illustrated above.
 *
 * @see <a href="https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/">LIBSVM datasets</a>
 */
class LibSVMDataSource private() {}
