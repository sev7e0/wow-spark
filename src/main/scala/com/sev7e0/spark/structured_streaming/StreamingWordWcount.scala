package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingWordWcount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamingWordWcount")
      .getOrCreate()

    import spark.implicits._

    //输入表
    val line = spark
      .readStream
      .format("socket")
      .option("host", "172.23.7.72")
      .option("port", 9999)
      .load()

    line.isStreaming
    line.printSchema()

    val word = line.as[String].flatMap(_.split(" "))

    //输出表
    val count = word.groupBy("value").count()

    val query = count.writeStream
      .outputMode(outputMode = "complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
