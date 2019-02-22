package com.lijiaqi.spark.structured_streaming

import org.apache.spark.sql.SparkSession

/**
  * no support for execution
  */
object WriteStream {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(WriteStream.getClass.getName)
      .master("local")
      .getOrCreate()
    val streamDF = session.readStream.load()


    /**
      * 使用writeStream则需要指定一些规则和模式
      *   1.queryName:非必须,指定一个唯一的名字,能够方便识别
      *   2.outputMode:非必须,输出模式,当前支持 Append mode(默认)/Complete mode/Update mode(从2.1.1开始)
      *   3.trigger:非必须,如未设置触发器,系统将在处理完成后立即检查数据可用性,如超时导致错过触发器,将在完成后立即触发
      *   4.format:生成格式,parquet/json/csv/kafka...
      */
    // File Sink ("orc", "json", "csv")
    streamDF.writeStream
        .queryName("test")
//      .trigger()
        .outputMode("complete")
      .format("parquet")
      .option("path", "/")
      .partitionBy()
      .start()

    // Kafka Sink
    streamDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark01:2133,spark02:2133")
      .option("topic", "updates")
      .start()

    //Console Sink(for debug)
    streamDF.writeStream
      .queryName("console")
      .format("console")
      .start()

    // Memory Sink Console Memory谨慎使用,仅适合数据量小的情况下适用
    streamDF.writeStream
      .queryName("memory")
      .format("memory")
      .start()
    /**
      * 一些sink不支持容错,不能保证数据的持久性,仅适合debug使用
      * 参考链接:
      *   http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
      */

    val deviceDF = session.readStream.load()

    //将接收数据打印屏幕
    deviceDF.writeStream
      .format("console")
      .start()

    deviceDF.writeStream
      .format("parquet")
      .option("checkpointLocation","/path/checkpoint/dir")
      .option("path","/path/output/dir")
      .start()

    val aggregationDF = deviceDF.groupBy("device").count()

    //聚合后数据结果到console
    aggregationDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    //默认查询name会作为table name
    aggregationDF.writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("device")
      .start()
    //根据生成的table进行查询
    session.sql("select * from device").show()

    /**
      * To Be Continued
      * http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch
      */


  }

}
