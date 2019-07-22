package com.sev7e0.spark.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object IntCountByStructured {
  val master = "local"
  val serverList = "localhost:9092"
  val kafka = "kafka"

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master(master)
      .appName(IntCountByStructured.toString)
      .getOrCreate()

    import spark.implicits._
    //此处一定要注意structured streaming集成kafka与spark streaming集成所需依赖不同
    val query = spark
      .readStream

      //input stream阶段，读取kafka
      .format(kafka)
      .option("kafka.bootstrap.servers", serverList)
      .option("subscribe", "randomCount")
      .load()

      //转换为DataSet[String]
      .selectExpr("CAST(value as STRING)")
      .as[String]

      //正常的word count
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

      //output stream阶段
      .writeStream
      .outputMode("complete")
      //触发器十秒
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()

    /**
      * 输出结果
      * -------------------------------------------
      * Batch: 3
      * -------------------------------------------
      * +-----+-----+
      * |value|count|
      * +-----+-----+
      * |    3|    4|
      * |    8|    6|
      * |    0|    3|
      * |    5|    3|
      * |    6|    2|
      * |    9|    3|
      * |    1|    2|
      * |    4|    2|
      * |    2|    2|
      * +-----+-----+
      */

    query.awaitTermination()
  }


}
