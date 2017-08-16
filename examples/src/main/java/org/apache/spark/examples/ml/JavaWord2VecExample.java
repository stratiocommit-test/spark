/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.ml;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaWord2VecExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWord2VecExample")
      .getOrCreate();

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
      RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
      RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> documentDF = spark.createDataFrame(data, schema);

    // Learn a mapping from words to Vectors.
    Word2Vec word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0);

    Word2VecModel model = word2Vec.fit(documentDF);
    Dataset<Row> result = model.transform(documentDF);

    for (Row row : result.collectAsList()) {
      List<String> text = row.getList(0);
      Vector vector = (Vector) row.get(1);
      System.out.println("Text: " + text + " => \nVector: " + vector + "\n");
    }
    // $example off$

    spark.stop();
  }
}
