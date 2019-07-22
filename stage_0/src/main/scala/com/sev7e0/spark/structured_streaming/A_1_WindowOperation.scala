package com.sev7e0.spark.structured_streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object A_1_WindowOperation {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println(s" Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<滑动间隔> 必须要小于或等于 <窗口间隔>")
    }

    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession.builder()
      .master("local")
      .appName(A_1_WindowOperation.getClass.getName)
      .getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
    import spark.implicits._

    val words = lines.as[(String, Timestamp)]
      .flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF()

    val windowCount = words.groupBy(
      window($"timestamp", windowDuration, slideDuration)
      , $"word").count().orderBy("window")

    val query = windowCount.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()


  }
}
