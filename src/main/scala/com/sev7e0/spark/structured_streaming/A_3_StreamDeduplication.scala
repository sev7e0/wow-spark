package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession

object A_3_StreamDeduplication {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("StreamDeduplication")
      .getOrCreate()
    val streamDF = session.readStream.load()

    //不适用水印的情况下操作guid列
    streamDF.dropDuplicates("guid")

    /**
      * 使用水印的情况下操作guid列
      * 在限定了重复记录的时间有上限是可以使用,操作水印时间的在不会被再操作
      */
    streamDF.withWatermark("eventTime", "2 seconds")
      .dropDuplicates("guid")

  }
}
