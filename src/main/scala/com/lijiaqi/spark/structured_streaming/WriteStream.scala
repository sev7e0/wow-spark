package com.lijiaqi.spark.structured_streaming

import org.apache.spark.sql.SparkSession

object WriteStream {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(WriteStream.getClass.getName)
      .master("local")
      .getOrCreate()
    val streamDF = session.readStream.load()


    //
    streamDF.writeStream
        .queryName("test")
//      .trigger()
        .outputMode("complete")
      .format("parquet")
      .option("path", "/")
      .start()


  }

}
