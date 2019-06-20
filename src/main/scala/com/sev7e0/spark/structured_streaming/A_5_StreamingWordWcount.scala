package com.sev7e0.spark.structured_streaming

import com.sev7e0.spark.spark_streaming.StreamingLogger
import org.apache.spark.sql.SparkSession

object A_5_StreamingWordWcount {

  val MASTER = "local"
  val HOST = "localhost"
  val PORT = 9999

  /**
    * 测试：
    * os：macOS
    * command：netcat -lp 9999
    *
    * 随意输入
    */
  def main(args: Array[String]): Unit = {

    //创建SparkSession对象
    val spark = SparkSession.builder()
      .appName(A_5_StreamingWordWcount.getClass.getName)
      .master(MASTER)
      .getOrCreate()

    StreamingLogger.setLoggerLevel()

    //输入表
    val line = spark
      .readStream
      .format("socket")
      .option("host", HOST)
      .option("port", PORT)
      .load()

    //打印结构
    line.printSchema()

    //DataFrame隐式转换为DataSet
    import spark.implicits._
    val word = line.as[String].flatMap(_.split(" "))

    //对流进行操作
    val count = word.groupBy("value").count()

    val query = count.writeStream
      .outputMode(outputMode = "complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
